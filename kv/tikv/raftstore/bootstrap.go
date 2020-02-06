package raftstore

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func isRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(startKey)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) < 0 {
				hasData = true
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return !hasData, err
}

func BootstrapStore(engines *engine_util.Engines, clusterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.Kv, MinKey, MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	empty, err = isRangeEmpty(engines.Raft, MinKey, MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clusterID
	ident.StoreId = storeID
	err = putMsg(engines.Kv, storeIdentKey, ident)
	if err != nil {
		return err
	}
	engines.SyncKVWAL()
	return nil
}

func PrepareBootstrap(engins *engine_util.Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
	err := writePrepareBootstrap(engins, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func writePrepareBootstrap(engines *engine_util.Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMsg(prepareBootstrapKey, state)
	kvWB.SetMsg(RegionStateKey(region.Id), state)
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	err = engines.SyncKVWAL()
	if err != nil {
		return err
	}
	raftWB := new(engine_util.WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return engines.SyncRaftWAL()
}

func writeInitialApplyState(kvWB *engine_util.WriteBatch, regionID uint64) {
	applyState := &rspb.RaftApplyState{
		AppliedIndex: RaftInitLogIndex,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: RaftInitLogIndex,
			Term:  RaftInitLogTerm,
		},
	}
	kvWB.SetMsg(ApplyStateKey(regionID), applyState)
}

func writeInitialRaftState(raftWB *engine_util.WriteBatch, regionID uint64) {
	raftState := &rspb.RaftLocalState{
		HardState: &eraftpb.HardState{
			Term:   RaftInitLogTerm,
			Commit: RaftInitLogIndex,
		},
		LastIndex: RaftInitLogIndex,
	}
	raftWB.SetMsg(RaftStateKey(regionID), raftState)
}

func ClearPrepareBootstrap(engines *engine_util.Engines, regionID uint64) error {
	err := engines.Raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(RaftStateKey(regionID))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	wb := new(engine_util.WriteBatch)
	wb.Delete(prepareBootstrapKey)
	// should clear raft initial state too.
	wb.Delete(RegionStateKey(regionID))
	wb.Delete(ApplyStateKey(regionID))
	err = engines.WriteKV(wb)
	if err != nil {
		return err
	}
	return engines.SyncKVWAL()
}

func ClearPrepareBootstrapState(engines *engine_util.Engines) error {
	err := engines.Kv.Update(func(txn *badger.Txn) error {
		return txn.Delete(prepareBootstrapKey)
	})
	engines.SyncKVWAL()
	return errors.WithStack(err)
}
