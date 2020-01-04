package raftstore

import (
	"encoding/binary"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pkg/eraftpb"
	"github.com/ngaut/unistore/pkg/raft_cmdpb"
	"github.com/ngaut/unistore/tikv/mvcc"
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
		err = restoreAppliedEntry(&entry, txn, bundle.LockStore, bundle.RollbackStore)
		if err != nil {
			return
		}
	})
	if iterCnt > 0 {
		log.Info("restore lock store iterated", iterCnt, "entries fromm offset", offset)
	}
	if err != nil {
		return err
	}
	return err1
}

func restoreAppliedEntry(entry *eraftpb.Entry, txn *badger.Txn, lockStore, rollbackStore *lockstore.MemStore) error {
	if entry.EntryType != eraftpb.EntryType_EntryNormal {
		return nil
	}
	var raftCmdRequest raft_cmdpb.RaftCmdRequest
	err := raftCmdRequest.Unmarshal(entry.Data)
	if err != nil {
		return err
	}
	if raftCmdRequest.AdminRequest != nil {
		return nil
	}
	if len(raftCmdRequest.Requests) == 0 {
		return nil
	}
	writeCmdOps := createWriteCmdOps(raftCmdRequest.Requests)
	for _, op := range writeCmdOps {
		switch x := op.(type) {
		case *prewriteOp:
			restorePrewrite(*x, txn, lockStore)
		case *commitOp:
			restoreCommit(*x, lockStore)
		case *rollbackOp:
			if rollbackStore != nil {
				restoreRollback(*x, rollbackStore)
			}
		default:
			log.Fatalf("invalid input op=%v", x)
		}
	}
	return nil
}

func restorePrewrite(op prewriteOp, txn *badger.Txn, lockStore *lockstore.MemStore) {
	key, value := convertPrewriteToLock(op, txn)
	lockStore.Insert(key, value)
}

func restoreCommit(op commitOp, lockStore *lockstore.MemStore) {
	_, rawKey, err := codec.DecodeBytes(op.delLock.Key, nil)
	if err != nil {
		panic(err)
	}
	lockStore.Delete(rawKey)
}

func restoreRollback(op rollbackOp, rollbackStore *lockstore.MemStore) {
	remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
	if err != nil {
		panic(err)
	}
	rollbackStore.Insert(append(rawKey, remain...), []byte{0})
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
