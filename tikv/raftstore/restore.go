package raftstore

import (
	"encoding/binary"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/tidb/util/codec"
)

func RestoreLockStore(offset uint64, bundle *mvcc.DBBundle, raftDB *badger.DB) error {
	appliedIndices := make(map[uint64]uint64)
	var err error
	txn := bundle.DB.NewTransaction(false)
	defer txn.Discard()
	iterCnt := 0
	err1 := raftDB.IterateVLog(offset, func(e badger.Entry) {
		iterCnt++
		if err != nil {
			return
		}
		key := y.ParseKey(e.Key)
		if !isRaftLogKey(key) {
			return
		}
		applied, err := isRaftLogApplied(key, appliedIndices, txn)
		if err != nil {
			return
		}
		if !applied {
			return
		}
		var entry eraftpb.Entry
		err = entry.Unmarshal(e.Value)
		if err != nil {
			return
		}
		if entry.EntryType != eraftpb.EntryType_EntryNormal {
			return
		}
		var raftCmdRequest raft_cmdpb.RaftCmdRequest
		err = raftCmdRequest.Unmarshal(entry.Data)
		if err != nil {
			return
		}
		if raftCmdRequest.AdminRequest != nil {
			return
		}
		if len(raftCmdRequest.Requests) == 0 {
			return
		}
		writeCmdOps := createWriteCmdOps(raftCmdRequest.Requests)
		for _, prewrite := range writeCmdOps.prewrites {
			restorePrewrite(prewrite, txn, bundle)
		}
		for _, commit := range writeCmdOps.commits {
			restoreCommit(commit, bundle)
		}
		for _, rollback := range writeCmdOps.rollbacks {
			restoreRollback(rollback, bundle)
		}
	})
	log.Info("restore lock store iterated", iterCnt, "entries")
	if err != nil {
		return err
	}
	return err1
}

func restorePrewrite(op prewriteOp, txn *badger.Txn, bundle *mvcc.DBBundle) {
	key, value := convertPrewriteToLock(op, txn)
	bundle.LockStore.Insert(key, value)
}

func restoreCommit(op commitOp, bundle *mvcc.DBBundle) {
	_, rawKey, err := codec.DecodeBytes(op.delLock.Key, nil)
	if err != nil {
		panic(err)
	}
	bundle.LockStore.Delete(rawKey)
}

func restoreRollback(op rollbackOp, bundle *mvcc.DBBundle) {
	remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
	if err != nil {
		panic(err)
	}
	bundle.RollbackStore.Insert(append(rawKey, remain...), []byte{0})
}

func isRaftLogKey(key []byte) bool {
	return len(key) == RegionRaftLogLen &&
		key[0] == LocalPrefix &&
		key[1] == RegionRaftPrefix &&
		key[10] == RaftLogSuffix
}

func isRaftLogApplied(key []byte, appliedIndices map[uint64]uint64, txn *badger.Txn) (bool, error) {
	regionID := binary.BigEndian.Uint64(key[2:])
	appliedIdx, ok := appliedIndices[regionID]
	if !ok {
		var err error
		appliedIdx, err = loadAppliedIdx(regionID, txn)
		if err != nil {
			return false, err
		}
		log.Info("region", regionID, "appliedIdx", appliedIdx)
		appliedIndices[regionID] = appliedIdx
	}
	idx := binary.BigEndian.Uint64(key[11:])
	return appliedIdx >= idx, nil
}

func loadAppliedIdx(regionID uint64, txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(ApplyStateKey(regionID))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			log.Info("region", regionID, "applied idx not found")
			return 0, nil
		}
		return 0, err
	}
	var val []byte
	val, err = item.Value()
	if err != nil {
		return 0, err
	}
	var state applyState
	state.Unmarshal(val)
	return state.appliedIndex, nil
}
