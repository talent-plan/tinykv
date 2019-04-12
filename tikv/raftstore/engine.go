package raftstore

import (
	"github.com/coocood/badger"
	"github.com/cznic/mathutil"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"math"
)

type DBBundle struct {
	db            *badger.DB
	lockStore     *lockstore.MemStore
	rollbackStore *lockstore.MemStore
}

type DBSnapshot struct {
	Txn           *badger.Txn
	LockStore     *lockstore.MemStore
	RollbackStore *lockstore.MemStore
}

func NewDBSnapshot(db *DBBundle) *DBSnapshot {
	return &DBSnapshot{
		Txn:           db.db.NewTransaction(false),
		LockStore:     db.lockStore,
		RollbackStore: db.rollbackStore,
	}
}

type RegionSnapshot struct {
	Region   *metapb.Region
	Snapshot *DBSnapshot
}

type Engines struct {
	kv       *DBBundle
	kvPath   string
	raft     *badger.DB
	raftPath string
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
	size          int
	safePoint     int
	safePointLock int
	safePointSize int
	lockEntries   []*badger.Entry
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

func (wb *WriteBatch) DeleteLock(key []byte) {
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
	wb.safePointLock = len(wb.entries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.lockEntries = wb.lockEntries[:wb.safePointLock]
	wb.size = wb.safePointSize
}

func (wb *WriteBatch) WriteToKV(bundle *DBBundle) error {
	if len(wb.entries) > 0 {
		err := bundle.db.Update(func(txn *badger.Txn) error {
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
	if len(wb.lockEntries) > 0 {
		for _, entry := range wb.lockEntries {
			switch entry.UserMeta[0] {
			case mvcc.LockUserMetaRollbackByte:
				bundle.rollbackStore.Insert(entry.Key, []byte{0})
			case mvcc.LockUserMetaDeleteByte:
				if !bundle.lockStore.Delete(entry.Key) {
					panic("failed to delete key")
				}
			case mvcc.LockUserMetaRollbackGCByte:
				bundle.rollbackStore.Delete(entry.Key)
			default:
				if !bundle.lockStore.Insert(entry.Key, entry.Value) {
					panic("failed to insert key")
				}
			}
		}
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

func (wb *WriteBatch) MustWriteToKV(db *DBBundle) {
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
}

// Todo, the following code redundant to unistore/tikv/worker.go, just as a place holder now.

const delRangeBatchSize = 4096

const maxSystemTS = math.MaxUint64

func deleteRange(db *DBBundle, startKey, endKey []byte) error {
	// Delete keys first.
	keys := make([][]byte, 0, delRangeBatchSize)
	oldStartKey := mvcc.EncodeOldKey(startKey, maxSystemTS)
	oldEndKey := mvcc.EncodeOldKey(endKey, maxSystemTS)
	txn := db.db.NewTransaction(false)
	reader := dbreader.NewDBReader(startKey, endKey, txn, 0)
	keys = collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
	keys = collectRangeKeys(reader.GetIter(), oldStartKey, oldEndKey, keys)
	reader.Close()
	if err := deleteKeysInBatch(db, keys, delRangeBatchSize); err != nil {
		return err
	}

	// Delete lock
	lockIte := db.lockStore.NewIterator()
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

func deleteKeysInBatch(db *DBBundle, keys [][]byte, batchSize int) error {
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

func deleteLocksInBatch(db *DBBundle, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		dbBatch := new(WriteBatch)
		for _, key := range batchKeys {
			dbBatch.DeleteLock(key)
		}
		if err := dbBatch.WriteToKV(db); err != nil {
			return err
		}
	}
	return nil
}

