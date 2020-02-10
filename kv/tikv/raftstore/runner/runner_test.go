package runner

import (
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copySnapshot is a helper function to copy snapshot.
// Only used in tests.
func copySnapshot(to, from snap.Snapshot) error {
	if !to.Exists() {
		_, err := io.Copy(to, from)
		if err != nil {
			return errors.WithStack(err)
		}
		return to.Save()
	}
	return nil
}

func newEnginesWithKVDb(t *testing.T, kv *badger.DB) *engine_util.Engines {
	engines := new(engine_util.Engines)
	engines.Kv = kv
	var err error
	engines.RaftPath, err = ioutil.TempDir("", "tinykv_raft")
	require.Nil(t, err)
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.RaftPath
	raftOpts.ValueDir = engines.RaftPath
	raftOpts.ValueThreshold = 256
	engines.Raft, err = badger.Open(raftOpts)
	require.Nil(t, err)
	return engines
}

func getTestDBForRegions(t *testing.T, path string, regions []uint64) *badger.DB {
	db := openDB(t, path)
	fillDBData(t, db)
	for _, regionID := range regions {
		// Put apply state into kv engine.
		applyState := &rspb.RaftApplyState{
			AppliedIndex: 10,
			TruncatedState: &rspb.RaftTruncatedState{
				Index: 10,
			},
		}
		require.Nil(t, engine_util.PutMsg(db, meta.ApplyStateKey(regionID), applyState))

		// Put region info into kv engine.
		region := genTestRegion(regionID, 1, 1)
		regionState := new(rspb.RegionLocalState)
		regionState.Region = region
		require.Nil(t, engine_util.PutMsg(db, meta.RegionStateKey(regionID), regionState))
	}
	return db
}

func genTestRegion(regionID, storeID, peerID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(""),
		EndKey:   []byte(""),
		RegionEpoch: &metapb.RegionEpoch{
			Version: 1,
			ConfVer: 1,
		},
		Peers: []*metapb.Peer{
			{StoreId: storeID, Id: peerID},
		},
	}
}

func openDB(t *testing.T, dir string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	return db
}

func fillDBData(t *testing.T, db *badger.DB) {
	// write some data for multiple cfs.
	wb := new(engine_util.WriteBatch)
	value := make([]byte, 32)
	wb.SetCF(engine_util.CfDefault, []byte("key"), value)
	wb.SetCF(engine_util.CfWrite, []byte("key"), value)
	wb.SetCF(engine_util.CfLock, []byte("key"), value)
	err := wb.WriteToDB(db)
	require.Nil(t, err)
}

func TestApplies(t *testing.T) {
	kvPath, err := ioutil.TempDir("", "testApplies")
	require.Nil(t, err)
	db := getTestDBForRegions(t, kvPath, []uint64{1, 2, 3, 4, 5, 6})
	keys := []byte{1, 2, 3, 4, 5, 6}
	for _, k := range keys {
		require.Nil(t, db.Update(func(txn *badger.Txn) error {
			require.Nil(t, txn.Set([]byte{k}, []byte{k}))
			require.Nil(t, txn.Set([]byte{k + 1}, []byte{k + 1}))
			// todo, there might be flush method needed.
			// todo, test also test level 0 files, we add later when badger export level 0 files api.
			return nil
		}))
	}

	engines := newEnginesWithKVDb(t, db)
	engines.KvPath = kvPath
	defer engines.Destroy()

	snapPath, err := ioutil.TempDir("", "tinykv_snap")
	defer os.RemoveAll(snapPath)
	require.Nil(t, err)
	mgr := snap.NewSnapManager(snapPath)
	wg := new(sync.WaitGroup)
	regionWorker := worker.NewWorker("snap-manager", wg)
	regionRunner := NewRegionTaskHandler(engines, mgr)
	regionWorker.Start(regionRunner)
	genAndApplySnap := func(regionId uint64) {
		tx := make(chan *eraftpb.Snapshot, 1)
		tsk := &worker.Task{
			Tp: worker.TaskTypeRegionGen,
			Data: &RegionTask{
				RegionId: regionId,
				Notifier: tx,
			},
		}
		regionWorker.Sender() <- *tsk
		s1 := <-tx
		data := s1.Data
		key := snap.SnapKeyFromRegionSnap(regionId, s1)
		mgr := snap.NewSnapManager(snapPath)
		s2, err := mgr.GetSnapshotForSending(key)
		require.Nil(t, err)
		s3, err := mgr.GetSnapshotForReceiving(key, data)
		require.Nil(t, err)
		require.Nil(t, copySnapshot(s3, s2))

		// set applying state
		wb := new(engine_util.WriteBatch)
		regionLocalState, err := meta.GetRegionLocalState(engines.Kv, regionId)
		require.Nil(t, err)
		regionLocalState.State = rspb.PeerState_Applying
		require.Nil(t, wb.SetMsg(meta.RegionStateKey(regionId), regionLocalState))
		require.Nil(t, wb.WriteToDB(engines.Kv))

		// apply snapshot
		var status = snap.JobStatus_Pending
		tsk2 := &worker.Task{
			Tp: worker.TaskTypeRegionApply,
			Data: &RegionTask{
				RegionId: regionId,
				Status:   &status,
			},
		}
		regionWorker.Sender() <- *tsk2
	}

	waitApplyFinish := func(regionId uint64) {
		for {
			time.Sleep(time.Millisecond * 100)
			regionLocalState, err := meta.GetRegionLocalState(engines.Kv, regionId)
			require.Nil(t, err)
			if regionLocalState.State == rspb.PeerState_Normal {
				break
			}
		}
	}

	genAndApplySnap(1)
	waitApplyFinish(1)

	genAndApplySnap(2)
	waitApplyFinish(2)

	genAndApplySnap(3)
	waitApplyFinish(3)
}

func TestGcRaftLog(t *testing.T) {
	engines := util.NewTestEngines()
	defer engines.Destroy()
	raftDb := engines.Raft
	taskResCh := make(chan raftLogGcTaskRes, 1)
	runner := raftLogGCTaskHandler{taskResCh: taskResCh}

	//  generate raft logs
	regionId := uint64(1)
	raftWb := new(engine_util.WriteBatch)
	for i := uint64(0); i < 100; i++ {
		k := meta.RaftLogKey(regionId, i)
		raftWb.Set(k, []byte("entry"))
	}
	raftWb.WriteToDB(raftDb)

	type tempHolder struct {
		raftLogGcTask     worker.Task
		expectedCollected uint64
		nonExistRange     [2]uint64
		existRange        [2]uint64
	}

	tbls := []tempHolder{
		{
			raftLogGcTask: worker.Task{
				Data: &RaftLogGCTask{
					RaftEngine: raftDb,
					RegionID:   regionId,
					StartIdx:   uint64(0),
					EndIdx:     uint64(10),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 10},
			existRange:        [...]uint64{10, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &RaftLogGCTask{
					RaftEngine: raftDb,
					RegionID:   regionId,
					StartIdx:   uint64(0),
					EndIdx:     uint64(50),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(40),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &RaftLogGCTask{
					RaftEngine: raftDb,
					RegionID:   regionId,
					StartIdx:   uint64(50),
					EndIdx:     uint64(50),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(0),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &RaftLogGCTask{
					RaftEngine: raftDb,
					RegionID:   regionId,
					StartIdx:   uint64(50),
					EndIdx:     uint64(60),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 60},
			existRange:        [...]uint64{60, 100},
		},
	}

	for _, h := range tbls {
		runner.Handle(h.raftLogGcTask)
		res := <-taskResCh
		assert.Equal(t, h.expectedCollected, uint64(res))
		raftLogMustNotExist(t, raftDb, 1, h.nonExistRange[0], h.nonExistRange[1])
		raftLogMustExist(t, raftDb, 1, h.existRange[0], h.existRange[1])
	}
}

func raftLogMustNotExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := meta.RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(k)
			assert.Equal(t, err, badger.ErrKeyNotFound)
			return nil
		})
	}
}

func raftLogMustExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := meta.RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(k)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			return nil
		})
	}
}
