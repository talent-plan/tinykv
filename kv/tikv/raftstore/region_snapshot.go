package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type regionSnapshot struct {
	regionState *raft_serverpb.RegionLocalState
	db          *badger.DB
	txn         *badger.Txn
	term        uint64
	index       uint64
}

func (en *Engines) newRegionSnapshot(engines engine_util.Engines, regionId uint64) (snap *regionSnapshot, err error) {
	txn := en.kv.NewTransaction(false)

	// Verify that the region version to make sure the start key and end key has not changed.
	regionState := new(raft_serverpb.RegionLocalState)
	val, err := getValueTxn(txn, RegionStateKey(regionId))
	if err != nil {
		return nil, err
	}
	err = regionState.Unmarshal(val)
	if err != nil {
		return nil, err
	}

	index, term, err := getAppliedIdxTermForSnapshot(engines.raft, txn, regionId)
	if err != nil {
		return nil, err
	}
	snap = &regionSnapshot{
		regionState: regionState,
		db:          engines.kv,
		txn:         txn,
		term:        term,
		index:       index,
	}
	return snap, nil
}
