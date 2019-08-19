package raftstore

import (
	"bytes"
	"math"

	"github.com/coocood/badger"
	"github.com/cznic/mathutil"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type regionSnapshot struct {
	regionState *raft_serverpb.RegionLocalState
	txn         *badger.Txn
	lockSnap    *lockstore.MemStore
	term        uint64
	index       uint64
}

func (rs *regionSnapshot) redoLocks(raft *badger.DB, redoIdx uint64) error {
	regionID := rs.regionState.Region.Id
	item, err := rs.txn.Get(ApplyStateKey(regionID))
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	var applyState applyState
	applyState.Unmarshal(val)
	appliedIdx := applyState.appliedIndex
	entries, _, err := fetchEntriesTo(raft, regionID, redoIdx, appliedIdx+1, math.MaxUint64, nil)
	if err != nil {
		return err
	}
	for i := range entries {
		err = restoreAppliedEntry(&entries[i], rs.txn, rs.lockSnap, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type Engines struct {
	kv       *mvcc.DBBundle
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func NewEngines(kvEngine *mvcc.DBBundle, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		kv: &mvcc.DBBundle{
			DB:            kvEngine.DB,
			LockStore:     kvEngine.LockStore,
			RollbackStore: kvEngine.RollbackStore,
		},
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
	}
}

func (en *Engines) newRegionSnapshot(regionId, redoIdx uint64) (snap *regionSnapshot, err error) {
	// We need to get the old region state out of the snapshot transaction to fetch data in lockStore.
	// The lockStore data must be fetch before we start the snapshot transaction to make sure there is no newer data
	// in the lockStore. The missing old data can be restored by raft log.
	oldRegionState, err := getRegionLocalState(en.kv.DB, regionId)
	if err != nil {
		return nil, err
	}
	lockSnap := lockstore.NewMemStore(8 << 20)
	iter := en.kv.LockStore.NewIterator()
	start, end := rawDataStartKey(oldRegionState.Region.StartKey), rawRegionKey(oldRegionState.Region.EndKey)
	for iter.Seek(start); iter.Valid() && (len(end) == 0 || bytes.Compare(iter.Key(), end) < 0); iter.Next() {
		lockSnap.Insert(iter.Key(), iter.Value())
	}

	txn := en.kv.DB.NewTransaction(false)

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
	if regionState.Region.RegionEpoch.Version != oldRegionState.Region.RegionEpoch.Version {
		return nil, errors.New("region changed during newRegionSnapshot")
	}

	index, term, err := getAppliedIdxTermForSnapshot(en.raft, txn, regionId)
	if err != nil {
		return nil, err
	}
	snap = &regionSnapshot{
		regionState: regionState,
		txn:         txn,
		lockSnap:    lockSnap,
		term:        term,
		index:       index,
	}
	err = snap.redoLocks(en.raft, redoIdx)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToKV(en.kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
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

type WriteBatch struct {
	entries       []*badger.Entry
	lockEntries   []*badger.Entry
	size          int
	safePoint     int
	safePointLock int
	safePointSize int
	safePointUndo int
}

func (wb *WriteBatch) Len() int {
	return len(wb.entries) + len(wb.lockEntries)
}

func (wb *WriteBatch) Set(key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
}

func (wb *WriteBatch) SetLock(key, val []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: mvcc.LockUserMetaNone,
	})
}

func (wb *WriteBatch) DeleteLock(key []byte, val []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		UserMeta: mvcc.LockUserMetaDelete,
	})
}

func (wb *WriteBatch) Rollback(key []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		UserMeta: mvcc.LockUserMetaRollback,
	})
}

func (wb *WriteBatch) SetWithUserMeta(key, val, useMeta []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: useMeta,
	})
	wb.size += len(key) + len(val) + len(useMeta)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetMsg(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointLock = len(wb.lockEntries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.lockEntries = wb.lockEntries[:wb.safePointLock]
	wb.size = wb.safePointSize
}

// WriteToKV flush WriteBatch to DB by two steps:
// 	1. Write entries to badger. After save ApplyState to badger, subsequent regionSnapshot will start at new raft index.
//	2. Update lockStore, the date in lockStore may be older than the DB, so we need to restore then entries from raft log.
func (wb *WriteBatch) WriteToKV(bundle *mvcc.DBBundle) error {
	if len(wb.entries) > 0 {
		err := bundle.DB.Update(func(txn *badger.Txn) error {
			for _, entry := range wb.entries {
				var err1 error
				if len(entry.UserMeta) == 0 && len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if len(wb.lockEntries) > 0 {
		bundle.MemStoreMu.Lock()
		for _, entry := range wb.lockEntries {
			switch entry.UserMeta[0] {
			case mvcc.LockUserMetaRollbackByte:
				bundle.RollbackStore.Insert(entry.Key, []byte{0})
			case mvcc.LockUserMetaDeleteByte:
				if !bundle.LockStore.Delete(entry.Key) {
					panic("failed to delete key")
				}
			case mvcc.LockUserMetaRollbackGCByte:
				bundle.RollbackStore.Delete(entry.Key)
			default:
				if !bundle.LockStore.Insert(entry.Key, entry.Value) {
					panic("failed to insert key")
				}
			}
		}
		bundle.MemStoreMu.Unlock()
	}
	return nil
}

func (wb *WriteBatch) WriteToRaft(db *badger.DB) error {
	if len(wb.entries) > 0 {
		err := db.Update(func(txn *badger.Txn) error {
			var err1 error
			for _, entry := range wb.entries {
				if len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (wb *WriteBatch) MustWriteToKV(db *mvcc.DBBundle) {
	err := wb.WriteToKV(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) MustWriteToRaft(db *badger.DB) {
	err := wb.WriteToRaft(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0]
	wb.lockEntries = wb.lockEntries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointLock = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}

// Todo, the following code redundant to unistore/tikv/worker.go, just as a place holder now.

const delRangeBatchSize = 4096

const maxSystemTS = math.MaxUint64

func deleteRange(db *mvcc.DBBundle, startKey, endKey []byte) error {
	// Delete keys first.
	keys := make([][]byte, 0, delRangeBatchSize)
	oldStartKey := mvcc.EncodeOldKey(startKey, maxSystemTS)
	oldEndKey := mvcc.EncodeOldKey(endKey, maxSystemTS)
	txn := db.DB.NewTransaction(false)
	reader := dbreader.NewDBReader(startKey, endKey, txn, 0)
	keys = collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
	keys = collectRangeKeys(reader.GetIter(), oldStartKey, oldEndKey, keys)
	reader.Close()
	if err := deleteKeysInBatch(db, keys, delRangeBatchSize); err != nil {
		return err
	}

	// Delete lock
	lockIte := db.LockStore.NewIterator()
	keys = keys[:0]
	keys = collectLockRangeKeys(lockIte, startKey, endKey, keys)
	return deleteLocksInBatch(db, keys, delRangeBatchSize)
}

func collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func collectLockRangeKeys(it *lockstore.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		key := safeCopy(it.Key())
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func deleteKeysInBatch(db *mvcc.DBBundle, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		dbBatch := new(WriteBatch)
		for _, key := range batchKeys {
			dbBatch.Delete(key)
		}
		if err := dbBatch.WriteToKV(db); err != nil {
			return err
		}
	}
	return nil
}

func deleteLocksInBatch(db *mvcc.DBBundle, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		dbBatch := new(WriteBatch)
		for _, key := range batchKeys {
			dbBatch.DeleteLock(key, nil)
		}
		if err := dbBatch.WriteToKV(db); err != nil {
			return err
		}
	}
	return nil
}
