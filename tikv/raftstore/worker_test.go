package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

func newEnginesWithKVDb(t *testing.T, kv *DBBundle) *Engines {
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
		require.Nil(t, db.db.Update(func(txn *badger.Txn) error {
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
	regionRunner := newRegionRunner(engines, mgr, 0, time.Duration(time.Second*0))
	worker.start(regionRunner)
	genAndApplySnap := func(regionId uint64) {
		tx := make(chan *eraftpb.Snapshot, 1)
		worker.scheduler <- task{
			tp: taskTypeRegionGen,
			data: &regionTask{
				regionId: regionId,
				notifier: tx,
			},
		}
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
		wb := new(WriteBatch)
		regionLocalState, err := getRegionLocalState(engines.kv.db, regionId)
		require.Nil(t, err)
		regionLocalState.State = rspb.PeerState_Applying
		require.Nil(t, wb.SetMsg(RegionStateKey(regionId), regionLocalState))
		require.Nil(t, wb.WriteToKV(engines.kv))

		// apply snapshot
		var status JobStatus = JobStatus_Pending
		worker.scheduler <- task{
			tp: taskTypeRegionApply,
			data: &regionTask{
				regionId: regionId,
				status:   &status,
			},
		}
	}

	waitApplyFinish := func(regionId uint64) {
		for {
			time.Sleep(time.Millisecond * 100)
			regionLocalState, err := getRegionLocalState(engines.kv.db, regionId)
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
	genAndApplySnap(3)
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

	// the last one pending task finished
	// todo, check cf num files at level 0 is 2
}
