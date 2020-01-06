package raftstore

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
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
		it := dbreader.NewIterator(txn, false, startKey, endKey)
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
	empty, err := isRangeEmpty(engines.kv.DB, MinKey, MaxKey)
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
	err = putMsg(engines.kv.DB, storeIdentKey, ident)
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
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	err = engines.SyncKVWAL()
	if err != nil {
		return err
	}
	raftWB := new(WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return engines.SyncRaftWAL()
}

func writeInitialApplyState(kvWB *WriteBatch, regionID uint64) {
	applyState := applyState{
		appliedIndex:   RaftInitLogIndex,
		truncatedIndex: RaftInitLogIndex,
		truncatedTerm:  RaftInitLogTerm,
	}
	kvWB.Set(ApplyStateKey(regionID), applyState.Marshal())
}

func writeInitialRaftState(raftWB *WriteBatch, regionID uint64) {
	raftState := raftState{
		lastIndex: RaftInitLogIndex,
		term:      RaftInitLogTerm,
		commit:    RaftInitLogIndex,
	}
	raftWB.Set(RaftStateKey(regionID), raftState.Marshal())
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
	err := engines.kv.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(prepareBootstrapKey)
	})
	engines.SyncKVWAL()
	return errors.WithStack(err)
}
