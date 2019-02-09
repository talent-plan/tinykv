package raftstore

import (
	"bytes"
	"github.com/coocood/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func isRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		if len(startKey) > 0 && len(endKey) > 0 {
			itOpts.StartKey = startKey
			itOpts.EndKey = endKey
		}
		it := txn.NewIterator(itOpts)
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

func BootstrapStore(engines *Engines, clussterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.kv, MinKey, MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	empty, err = isRangeEmpty(engines.raft, MinKey, MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clussterID
	ident.StoreId = storeID
	err = putMsg(engines.kv, storeIdentKey, ident)
	if err != nil {
		return err
	}
	engines.SyncKVWAL()
	return nil
}

func PrepareBootstrap(engins *Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
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

func writePrepareBootstrap(engines *Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(WriteBatch)
	kvWB.SetMsg(prepareBootstrapKey, state)
	kvWB.SetMsg(RegionStateKey(region.Id), state)
	err := writeInitialApplyState(kvWB, region.Id)
	if err != nil {
		return err
	}
	err = engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	err = engines.SyncKVWAL()
	if err != nil {
		return err
	}
	raftWB := new(WriteBatch)
	err = writeInitialRaftState(raftWB, region.Id)
	if err != nil {
		return err
	}
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return engines.SyncRaftWAL()
}

func writeInitialApplyState(kvWB *WriteBatch, regionID uint64) error {
	raftState := &rspb.RaftApplyState{
		AppliedIndex: RaftInitLogIndex,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: RaftInitLogIndex,
			Term:  RaftInitLogTerm,
		},
	}
	return kvWB.SetMsg(ApplyStateKey(regionID), raftState)
}

func writeInitialRaftState(raftWB *WriteBatch, regionID uint64) error {
	raftState := &rspb.RaftLocalState{
		LastIndex: RaftInitLogIndex,
		HardState: &eraftpb.HardState{
			Term:   RaftInitLogTerm,
			Commit: RaftInitLogIndex,
		},
	}
	return raftWB.SetMsg(RaftStateKey(regionID), raftState)
}

func ClearPrepareBootstrap(engines *Engines, regionID uint64) error {
	err := engines.raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(RaftStateKey(regionID))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	wb := new(WriteBatch)
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

func ClearPrepareBootstrapState(engines *Engines) error {
	err := engines.kv.Update(func(txn *badger.Txn) error {
		return txn.Delete(prepareBootstrapKey)
	})
	engines.SyncKVWAL()
	return errors.WithStack(err)
}
