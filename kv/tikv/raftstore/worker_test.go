package raftstore

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/lockstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStalePeerInfo(t *testing.T) {
	timeout := time.Now()
	regionId := uint64(1)
	endKey := []byte{1, 2, 10, 10, 101, 10, 1, 9}
	peerInfo := newStalePeerInfo(regionId, endKey, timeout)
	assert.Equal(t, peerInfo.regionId(), regionId)
	assert.Equal(t, peerInfo.endKey(), endKey)
	assert.True(t, peerInfo.timeout().Equal(timeout))

	regionId = 101
	endKey = []byte{102, 11, 98, 23, 45, 76, 14, 43}
	timeout = time.Now().Add(time.Millisecond * 101)
	peerInfo.setRegionId(regionId)
	peerInfo.setEndKey(endKey)
	peerInfo.setTimeout(timeout)
	assert.Equal(t, peerInfo.regionId(), regionId)
	assert.Equal(t, peerInfo.endKey(), endKey)
	assert.True(t, peerInfo.timeout().Equal(timeout))
}

func insertRange(delRanges *pendingDeleteRanges, id uint64, s, e string, timeout time.Time) {
	delRanges.insert(id, []byte(s), []byte(e), timeout)
}

func TestPendingDeleteRanges(t *testing.T) {
	delRange := &pendingDeleteRanges{
		ranges: lockstore.NewMemStore(1024),
	}
	delay := time.Millisecond * 100
	id := uint64(0)
	timeout := time.Now().Add(delay)

	insertRange(delRange, id, "a", "c", timeout)
	insertRange(delRange, id, "m", "n", timeout)
	insertRange(delRange, id, "x", "z", timeout)
	insertRange(delRange, id+1, "f", "i", timeout)
	insertRange(delRange, id+1, "p", "t", timeout)
	assert.Equal(t, delRange.ranges.Len(), 5)

	time.Sleep(delay / 2)

	//  a____c    f____i    m____n    p____t    x____z
	//              g___________________q
	// when we want to insert [g, q), we first extract overlap ranges,
	// which are [f, i), [m, n), [p, t)
	timeout = time.Now().Add(delay)
	overlapRanges := delRange.drainOverlapRanges([]byte{'g'}, []byte{'q'})
	assert.Equal(t, overlapRanges, []delRangeHolder{
		{regionId: id + 1, startKey: []byte{'f'}, endKey: []byte{'i'}},
		{regionId: id, startKey: []byte{'m'}, endKey: []byte{'n'}},
		{regionId: id + 1, startKey: []byte{'p'}, endKey: []byte{'t'}},
	})

	assert.Equal(t, delRange.ranges.Len(), 2)
	insertRange(delRange, id+2, "g", "q", timeout)
	assert.Equal(t, delRange.ranges.Len(), 3)
	time.Sleep(delay / 2)

	// at t1, [a, c) and [x, z) will timeout
	now := time.Now()
	ranges := delRange.timeoutRanges(now)
	assert.Equal(t, ranges, []delRangeHolder{
		{regionId: id, startKey: []byte{'a'}, endKey: []byte{'c'}},
		{regionId: id, startKey: []byte{'x'}, endKey: []byte{'z'}},
	})

	for _, r := range ranges {
		delRange.remove(r.startKey)
	}
	assert.Equal(t, delRange.ranges.Len(), 1)

	time.Sleep(delay / 2)

	// at t2, [g, q) will timeout
	now = time.Now()
	ranges = delRange.timeoutRanges(now)
	assert.Equal(t, ranges, []delRangeHolder{
		{regionId: id + 2, startKey: []byte{'g'}, endKey: []byte{'q'}},
	})
	for _, r := range ranges {
		delRange.remove(r.startKey)
	}
	assert.Equal(t, delRange.ranges.Len(), 0)
}

func newEnginesWithKVDb(t *testing.T, kv *mvcc.DBBundle) *Engines {
	engines := new(Engines)
	engines.kv = kv
	var err error
	engines.raftPath, err = ioutil.TempDir("", "unistore_raft")
	require.Nil(t, err)
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.raftPath
	raftOpts.ValueDir = engines.raftPath
	raftOpts.ValueThreshold = 256
	engines.raft, err = badger.Open(raftOpts)
	require.Nil(t, err)
	return engines
}

func TestPendingApplies(t *testing.T) {
	kvPath, err := ioutil.TempDir("", "testPendingApplies")
	require.Nil(t, err)
	db := getTestDBForRegions(t, kvPath, []uint64{1, 2, 3, 4, 5, 6})
	keys := []byte{1, 2, 3, 4, 5, 6}
	for _, k := range keys {
		require.Nil(t, db.DB.Update(func(txn *badger.Txn) error {
			require.Nil(t, txn.Set([]byte{k}, []byte{k}))
			require.Nil(t, txn.Set([]byte{k + 1}, []byte{k + 1}))
			// todo, there might be flush method needed.
			// todo, test also test level 0 files, we add later when badger export level 0 files api.
			return nil
		}))
	}

	engines := newEnginesWithKVDb(t, db)
	engines.kvPath = kvPath
	defer cleanUpTestEngineData(engines)

	snapPath, err := ioutil.TempDir("", "unistore_snap")
	defer os.RemoveAll(snapPath)
	require.Nil(t, err)
	mgr := NewSnapManager(snapPath, nil)
	wg := new(sync.WaitGroup)
	worker := newWorker("snap-manager", wg)
	regionRunner := newRegionTaskHandler(engines, mgr, 0, time.Duration(time.Second*0))
	worker.start(regionRunner)
	genAndApplySnap := func(regionId uint64) {
		tx := make(chan *eraftpb.Snapshot, 1)
		tsk := &task{
			tp: taskTypeRegionGen,
		}
		rgTsk := &regionTask{
			regionId: regionId,
			notifier: tx,
		}
		txn := engines.kv.DB.NewTransaction(false)
		// TODO [fix this] the new regionTask need "redoIdx" as input param
		index, _, err := getAppliedIdxTermForSnapshot(engines.raft, txn, regionId)
		rgTsk.redoIdx = index + 1
		tsk.data = rgTsk
		require.Nil(t, err)
		worker.sender <- *tsk
		s1 := <-tx
		data := s1.Data
		key := SnapKeyFromRegionSnap(regionId, s1)
		mgr := NewSnapManager(snapPath, nil)
		s2, err := mgr.GetSnapshotForSending(key)
		require.Nil(t, err)
		s3, err := mgr.GetSnapshotForReceiving(key, data)
		require.Nil(t, err)
		require.Nil(t, copySnapshot(s3, s2))

		// set applying state
		wb := new(engine_util.WriteBatch)
		regionLocalState, err := getRegionLocalState(engines.kv.DB, regionId)
		require.Nil(t, err)
		regionLocalState.State = rspb.PeerState_Applying
		require.Nil(t, wb.SetMsg(RegionStateKey(regionId), regionLocalState))
		require.Nil(t, wb.WriteToKV(engines.kv))

		// apply snapshot
		var status = JobStatus_Pending
		tsk2 := &task{
			tp: taskTypeRegionApply,
			data: &regionTask{
				regionId: regionId,
				status:   &status,
			},
		}
		worker.sender <- *tsk2
	}

	waitApplyFinish := func(regionId uint64) {
		for {
			time.Sleep(time.Millisecond * 100)
			regionLocalState, err := getRegionLocalState(engines.kv.DB, regionId)
			require.Nil(t, err)
			if regionLocalState.State == rspb.PeerState_Normal {
				break
			}
		}
	}

	// snapshot will not ingest cause already write stall
	genAndApplySnap(1)
	// todo, test needed, cf num files at level 0 should be 6.

	// compact all files to the bottomest level
	// todo, compact files and then check that cf num files at level 0 should be 0.
	waitApplyFinish(1)

	// the pending apply task should be finished and snapshots are ingested.
	// note that when ingest sst, it may flush memtable if overlap,
	// so here will two level 0 files.
	// todo, check cf num files at level 0 is 2.

	// no write stall, ingest without delay
	genAndApplySnap(2)
	waitApplyFinish(2)
	// todo, check cf num files at level 0 is 4.

	// snapshot will not ingest cause it may cause write stall
	// TODO now this does not cause write stall, since `snapShotContext`.`ingestMaybeStall` not
	// implemented yet
	genAndApplySnap(3)
	waitApplyFinish(3)
	/*
		// todo, check cf num files at level 0 is 4.
		waitApplyFinish(4)
		// todo, check cf num files at level 0 is 4.
		genAndApplySnap(5)
		// todo, check cf num files at level 0 is 4.

		// compact all files to the bottomest level
		// todo compact files in range and then check cf num files at level 0 is 0.

		// make sure have checked pending applies
		waitApplyFinish(4)

		// before two pending apply tasks should be finished and snapshots are ingested
		// and one still in pending.
		// todo, check cf num files at level 0 is 4.

		// make sure have checked pending applies
		// todo compact files in range and then check cf num files at level 0 is 0.

		waitApplyFinish(5)
	*/

	// the last one pending task finished
	// todo, check cf num files at level 0 is 2
}

func TestGcRaftLog(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	raftDb := engines.raft
	taskResCh := make(chan raftLogGcTaskRes, 1)
	runner := raftLogGCTaskHandler{taskResCh: taskResCh}

	//  generate raft logs
	regionId := uint64(1)
	raftWb := new(engine_util.WriteBatch)
	for i := uint64(0); i < 100; i++ {
		k := RaftLogKey(regionId, i)
		raftWb.Set(k, []byte("entry"))
	}
	raftWb.WriteToRaft(raftDb)

	type tempHolder struct {
		raftLogGcTask     task
		expectedCollected uint64
		nonExistRange     [2]uint64
		existRange        [2]uint64
	}

	tbls := []tempHolder{
		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(10),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 10},
			existRange:        [...]uint64{10, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(50),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(40),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(50),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(0),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(60),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 60},
			existRange:        [...]uint64{60, 100},
		},
	}

	for _, h := range tbls {
		runner.handle(h.raftLogGcTask)
		res := <-taskResCh
		assert.Equal(t, h.expectedCollected, uint64(res))
		raftLogMustNotExist(t, raftDb, 1, h.nonExistRange[0], h.nonExistRange[1])
		raftLogMustExist(t, raftDb, 1, h.existRange[0], h.existRange[1])
	}
}

func raftLogMustNotExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(k)
			assert.Equal(t, err, badger.ErrKeyNotFound)
			return nil
		})
	}
}

func raftLogMustExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(k)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			return nil
		})
	}
}
