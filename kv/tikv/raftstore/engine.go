package raftstore

import (
	"math"

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

type Engines struct {
	kv       *badger.DB
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		kv:       kvEngine,
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
	}
}

func (en *Engines) newRegionSnapshot(regionId uint64) (snap *regionSnapshot, err error) {
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

	index, term, err := getAppliedIdxTermForSnapshot(en.raft, txn, regionId)
	if err != nil {
		return nil, err
	}
	snap = &regionSnapshot{
		regionState: regionState,
		db:          en.kv,
		txn:         txn,
		term:        term,
		index:       index,
	}
	return snap, nil
}

func (en *Engines) WriteKV(wb *engine_util.WriteBatch) error {
	return wb.WriteToKV(en.kv)
}

func (en *Engines) WriteRaft(wb *engine_util.WriteBatch) error {
	return wb.WriteToRaft(en.raft)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

// Todo, the following code redundant to unistore/tikv/worker.go, just as a place holder now.

const delRangeBatchSize = 4096

const maxSystemTS = math.MaxUint64
