package raftstore

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEnginesWithKVDb(t *testing.T, kv *badger.DB) *engine_util.Engines {
	engines := new(engine_util.Engines)
	engines.Kv = kv
	var err error
	engines.RaftPath, err = ioutil.TempDir("", "unistore_raft")
	require.Nil(t, err)
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.RaftPath
	raftOpts.ValueDir = engines.RaftPath
	raftOpts.ValueThreshold = 256
	engines.Raft, err = badger.Open(raftOpts)
	require.Nil(t, err)
	return engines
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
	defer cleanUpTestEngineData(engines)

	snapPath, err := ioutil.TempDir("", "unistore_snap")
	defer os.RemoveAll(snapPath)
	require.Nil(t, err)
	mgr := NewSnapManager(snapPath, nil)
	wg := new(sync.WaitGroup)
	regionWorker := worker.NewWorker("snap-manager", wg)
	regionRunner := newRegionTaskHandler(engines, mgr)
	regionWorker.Start(regionRunner)
	genAndApplySnap := func(regionId uint64) {
		tx := make(chan *eraftpb.Snapshot, 1)
		tsk := &worker.Task{
			Tp: worker.TaskTypeRegionGen,
			Data: &regionTask{
				regionId: regionId,
				notifier: tx,
			},
		}
		regionWorker.Sender() <- *tsk
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
		regionLocalState, err := getRegionLocalState(engines.Kv, regionId)
		require.Nil(t, err)
		regionLocalState.State = rspb.PeerState_Applying
		require.Nil(t, wb.SetMsg(RegionStateKey(regionId), regionLocalState))
		require.Nil(t, wb.WriteToKV(engines.Kv))

		// apply snapshot
		var status = JobStatus_Pending
		tsk2 := &worker.Task{
			Tp: worker.TaskTypeRegionApply,
			Data: &regionTask{
				regionId: regionId,
				status:   &status,
			},
		}
		regionWorker.Sender() <- *tsk2
	}

	waitApplyFinish := func(regionId uint64) {
		for {
			time.Sleep(time.Millisecond * 100)
			regionLocalState, err := getRegionLocalState(engines.Kv, regionId)
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
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	raftDb := engines.Raft
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
		raftLogGcTask     worker.Task
		expectedCollected uint64
		nonExistRange     [2]uint64
		existRange        [2]uint64
	}

	tbls := []tempHolder{
		{
			raftLogGcTask: worker.Task{
				Data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(10),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 10},
			existRange:        [...]uint64{10, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(50),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(40),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(50),
				},
				Tp: worker.TaskTypeRaftLogGC,
			},
			expectedCollected: uint64(0),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: worker.Task{
				Data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(60),
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
