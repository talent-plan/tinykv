package tikv

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/dgraph-io/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	db *badger.DB
}

func (store *MVCCStore) Get(key []byte, startTS uint64) ([]byte, error) {
	var result valueResult
	err := store.db.View(func(txn *badger.Txn) error {
		result = store.mvGet(txn, nil, key, startTS)
		return nil
	})
	if result.err == nil {
		result.err = errors.Trace(err)
	}
	return result.value, result.err
}

func (store *MVCCStore) newIterator(txn *badger.Txn) *badger.Iterator {
	var itOpts = badger.DefaultIteratorOptions
	itOpts.PrefetchValues = false
	return txn.NewIterator(itOpts)
}

type valueResult struct {
	commitTS uint64
	value    []byte
	err      error
}

func (store *MVCCStore) mvGet(txn *badger.Txn, iter *badger.Iterator, key []byte, startTS uint64) (result valueResult) {
	mvKey := codec.EncodeBytes(nil, key)
	item, err := txn.Get(mvKey)
	if err != nil && err != badger.ErrKeyNotFound {
		result.err = errors.Trace(err)
		return
	}
	if err == badger.ErrKeyNotFound {
		return
	}
	mixed, err1 := decodeMixed(item)
	if err1 != nil {
		result.err = errors.Trace(err)
		return
	}
	if mixed.hasLock() {
		result.err = store.checkLock(mixed.lock, key, startTS)
		if result.err != nil {
			return
		}
	}
	if !mixed.hasValue() {
		return
	}
	mvVal := mixed.val
	if mvVal.commitTS <= startTS {
		result.commitTS = mvVal.commitTS
		result.value = mvVal.value
		return
	}
	oldKey := encodeOldKeyFromMVKey(mvKey, startTS)
	if iter == nil {
		iter = store.newIterator(txn)
	}
	iter.Seek(oldKey)
	if !iter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return
	}
	item = iter.Item()
	mvVal, err = decodeValue(item)
	if err != nil {
		result.err = errors.Trace(err)
		return
	}
	result.commitTS = mvVal.commitTS
	result.value = mvVal.value
	return
}

func (store *MVCCStore) checkLock(lock mvccLock, key []byte, startTS uint64) error {
	lockVisible := lock.startTS < startTS
	isWriteLock := lock.op == kvrpcpb.Op_Put || lock.op == kvrpcpb.Op_Del
	isPrimaryGet := lock.startTS == lockVer && bytes.Equal(lock.primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return &ErrLocked{
			Key:     key,
			StartTS: lock.startTS,
			Primary: lock.primary,
			TTL:     lock.ttl,
		}
	}
	return nil
}

func (store *MVCCStore) BatchGet(keys [][]byte, startTS uint64) []Pair {
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for _, key := range keys {
			result := store.mvGet(txn, iter, key, startTS)
			if len(result.value) == 0 {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Value: result.value, Err: result.err})
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return []Pair{{Err: err}}
	}
	return pairs
}

func (store *MVCCStore) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	errs := make([]error, 0, len(mutations))
	err := updateWithRetry(store.db, func(txn *badger.Txn) error {
		errs = errs[:0]
		for _, m := range mutations {
			err1 := store.prewriteMutation(txn, m, primary, startTS, ttl)
			errs = append(errs, err1)
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return []error{err}
	}
	return errs
}

const lockVer uint64 = math.MaxUint64

func (store *MVCCStore) prewriteMutation(txn *badger.Txn, mutation *kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) error {
	mvKey := codec.EncodeBytes(nil, mutation.Key)
	item, err := txn.Get(mvKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	var mixed mixedValue
	if item != nil {
		mixed, err = decodeMixed(item)
		if err != nil {
			return errors.Trace(err)
		}
		if mixed.hasLock() {
			lock := mixed.lock
			if lock.op != kvrpcpb.Op_Rollback {
				return ErrRetryable("try again later")
			}
			if lock.startTS >= startTS {
				return ErrAbort("already rollback")
			}
			// If a rollback lock has a smaller start ts, we can overwrite it.
		}
		if mixed.hasValue() {
			mvVal := mixed.val
			if mvVal.commitTS > startTS {
				return ErrRetryable("write conflict")
			}
		}
	}
	mixed.lock = mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.Op,
		ttl:     ttl,
	}
	mixed.mixedType |= mixedLockFlag
	return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
}

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(keys [][]byte, startTS, commitTS uint64, diff *int64) error {
	var tmpDiff int64
	err := updateWithRetry(store.db, func(txn *badger.Txn) error {
		tmpDiff = 0
		for _, key := range keys {
			err1 := store.commitKey(txn, key, startTS, commitTS, &tmpDiff)
			if err1 != nil {
				return err1
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	atomic.AddInt64(diff, tmpDiff)
	return nil
}

func (store *MVCCStore) commitKey(txn *badger.Txn, key []byte, startTS, commitTS uint64, diff *int64) error {
	mvKey := codec.EncodeBytes(nil, key)
	item, err := txn.Get(mvKey)
	if err != nil {
		return errors.Trace(err)
	}
	mixed, err := decodeMixed(item)
	if !mixed.hasLock() {
		return errors.New("lock not found")
	}
	lock := mixed.lock
	if lock.startTS != startTS {
		return errors.New("replaced by another transaction")
	}
	if lock.op == kvrpcpb.Op_Rollback {
		return errors.New("already rollback")
	}
	return store.commitLock(txn, mvKey, mixed, startTS, commitTS, diff)
}

func (store *MVCCStore) commitLock(txn *badger.Txn, mvKey []byte, mixed mixedValue, startTS, commitTS uint64, diff *int64) error {
	lock := mixed.lock
	if lock.op == kvrpcpb.Op_Lock {
		return store.commitMixed(txn, mvKey, mixed, nil)
	}
	if mixed.hasValue() {
		val := mixed.val
		oldDataKey := encodeOldKeyFromMVKey(mvKey, val.commitTS)
		err := txn.Set(oldDataKey, val.MarshalBinary())
		if err != nil {
			return errors.Trace(err)
		}
	}
	var valueType mvccValueType
	if lock.op == kvrpcpb.Op_Put {
		valueType = typePut
	} else {
		valueType = typeDelete
	}
	mixed.mixedType |= mixedValueFlag
	mixed.val = mvccValue{
		valueType: valueType,
		startTS:   startTS,
		commitTS:  commitTS,
		value:     lock.value,
	}
	return store.commitMixed(txn, mvKey, mixed, diff)
}

func (store *MVCCStore) commitMixed(txn *badger.Txn, mvKey []byte, mixed mixedValue, diff *int64) error {
	rollbackTS := mixed.lock.rollbackTS
	if rollbackTS != 0 {
		// The rollback info is appended to the lock, we should reserve a rollback lock.
		mixed.lock = mvccLock{
			startTS: rollbackTS,
			op:      kvrpcpb.Op_Rollback,
		}
	} else {
		mixed.unsetLock()
	}
	mixedBin := mixed.MarshalBinary()
	if diff != nil {
		*diff += int64(len(mvKey) + len(mixedBin))
	}
	return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
}

func (store *MVCCStore) Rollback(keys [][]byte, startTS uint64) error {
	err1 := updateWithRetry(store.db, func(txn *badger.Txn) error {
		for _, key := range keys {
			err := store.rollbackKey(txn, key, startTS)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error(err1)
	}
	return err1
}

func (store *MVCCStore) rollbackKey(txn *badger.Txn, key []byte, startTS uint64) error {
	mvKey := encodeMVKey(key)
	item, err := txn.Get(mvKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		// The prewrite request is not arrived, we write a rollback lock to prevent the future prewrite.
		mixed := mixedValue{
			mixedType: mixedLockFlag,
			lock: mvccLock{
				startTS: startTS,
				op:      kvrpcpb.Op_Rollback,
			}}
		return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
	}
	mixed, err1 := decodeMixed(item)
	if err1 != nil {
		return errors.Trace(err1)
	}
	if mixed.hasLock() {
		lock := mixed.lock
		if lock.startTS < startTS {
			if lock.rollbackTS >= startTS {
				return nil
			}
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should append the startTS to the lock as rollbackTS.
			lock.rollbackTS = startTS
			return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
		}
		if lock.startTS == startTS {
			if lock.op == kvrpcpb.Op_Rollback {
				return nil
			}
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			mixed.lock = mvccLock{startTS: startTS, op: kvrpcpb.Op_Rollback}
			return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
		}
	}
	if !mixed.hasValue() {
		return nil
	}
	val := mixed.val
	if val.startTS == startTS {
		return ErrAlreadyCommitted(val.commitTS)
	}
	if val.startTS < startTS {
		// Prewrite and commit have not arrived.
		mixed.lock = mvccLock{startTS: startTS, op: kvrpcpb.Op_Rollback}
		mixed.mixedType |= mixedLockFlag
		return txn.SetWithMeta(mvKey, mixed.MarshalBinary(), mixed.mixedType)
	}
	// Look for the key in the old version.
	iter := store.newIterator(txn)
	oldKey := encodeOldKeyFromMVKey(mvKey, val.commitTS)
	// find greater commit version.
	for iter.Seek(oldKey); iter.ValidForPrefix(oldKey[:len(oldKey)-8]); iter.Next() {
		item := iter.Item()
		foundMvccKey := item.Key()
		if isVisibleKey(foundMvccKey, startTS) {
			break
		}
		_, ts, err := codec.DecodeUintDesc(foundMvccKey[len(foundMvccKey)-8:])
		if err != nil {
			return errors.Trace(err)
		}
		mvVal, err := decodeValue(item)
		if mvVal.startTS == startTS {
			return ErrAlreadyCommitted(ts)
		}
	}
	return nil
}

func (store *MVCCStore) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		mvStartKey := encodeMVKey(startKey)
		mvEndKey := encodeMVKey(endKey)
		var oldIter *badger.Iterator
		for iter.Seek(mvStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if exceedEndKey(item.Key(), mvEndKey) {
				return nil
			}
			mixed, err1 := decodeMixed(item)
			if err1 != nil {
				return errors.Trace(err1)
			}
			rawKey, err1 := decodeRawKey(item.Key())
			if err1 != nil {
				return errors.Trace(err1)
			}
			if mixed.hasLock() {
				err1 = store.checkLock(mixed.lock, rawKey, startTS)
				if err1 != nil {
					return errors.Trace(err1)
				}
			}
			if !mixed.hasValue() {
				continue
			}
			mvVal := mixed.val
			if mvVal.commitTS > startTS {
				if oldIter == nil {
					oldIter = store.newIterator(txn)
				}
				mvVal, err1 = store.getOldValue(oldIter, encodeOldKeyFromMVKey(item.Key(), startTS))
				if err1 == badger.ErrKeyNotFound {
					continue
				}
			}
			if mvVal.valueType == typeDelete {
				continue
			}
			pairs = append(pairs, Pair{Key: rawKey, Value: mvVal.value})
			if len(pairs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return pairs
}

func (store *MVCCStore) getOldValue(oldIter *badger.Iterator, oldKey []byte) (mvccValue, error) {
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return mvccValue{}, badger.ErrKeyNotFound
	}
	return decodeValue(oldIter.Item())
}

func isLockKey(mvKey []byte) bool {
	return len(mvKey) > 8 && binary.BigEndian.Uint64(mvKey[len(mvKey)-8:]) == 0
}

func isVisibleKey(mvKey []byte, startTS uint64) bool {
	ts := ^(binary.BigEndian.Uint64(mvKey[len(mvKey)-8:]))
	return startTS >= ts
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (store *MVCCStore) ReverseScan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		var opts badger.IteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		dataStartKey := encodeMVKey(startKey)
		dataEndKey := encodeMVKey(endKey)
		var oldIter *badger.Iterator
		for iter.Seek(dataEndKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if bytes.Compare(item.Key(), dataStartKey) < 0 {
				return nil
			}
			mixed, err1 := decodeMixed(item)
			if err1 != nil {
				return errors.Trace(err1)
			}
			rawKey, err1 := decodeRawKey(item.Key())
			if err1 != nil {
				return errors.Trace(err1)
			}
			if mixed.hasLock() {
				err1 = store.checkLock(mixed.lock, rawKey, startTS)
				if err1 != nil {
					return errors.Trace(err1)
				}
			}
			if !mixed.hasValue() {
				continue
			}
			mvVal := mixed.val
			if mvVal.commitTS > startTS {
				if oldIter == nil {
					oldIter = store.newIterator(txn)
				}
				mvVal, err1 = store.getOldValue(oldIter, encodeOldKeyFromMVKey(item.Key(), startTS))
				if err1 == badger.ErrKeyNotFound {
					continue
				}
			}
			if mvVal.valueType == typeDelete {
				continue
			}
			pairs = append(pairs, Pair{Key: rawKey, Value: mvVal.value})
			if len(pairs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return nil
}

func (store *MVCCStore) Cleanup(key []byte, startTS uint64) error {
	err := updateWithRetry(store.db, func(txn *badger.Txn) error {
		return store.rollbackKey(txn, key, startTS)
	})
	return err
}

func (store *MVCCStore) ScanLock(mvStartKey, mvEndKey []byte, limit int, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	err1 := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for iter.Seek(mvStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if exceedEndKey(item.Key(), mvEndKey) {
				return nil
			}
			if item.UserMeta()&mixedLockFlag == 0 {
				continue
			}
			mixed, err := decodeMixed(item)
			if err != nil {
				return errors.Trace(err)
			}
			lock := mixed.lock
			if lock.op == kvrpcpb.Op_Rollback {
				continue
			}
			if lock.startTS < maxTS {
				locks = append(locks, &kvrpcpb.LockInfo{
					PrimaryLock: lock.primary,
					LockVersion: lock.startTS,
					Key:         item.KeyCopy(nil),
					LockTtl:     lock.ttl,
				})
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error(err1)
	}
	return nil, nil
}

func (store *MVCCStore) ResolveLock(mvStartKey, mvEndKey []byte, startTS, commitTS uint64, diff *int64) error {
	var lockKeys [][]byte
	var lockVers []uint64
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for iter.Seek(mvStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if exceedEndKey(item.Key(), mvEndKey) {
				return nil
			}
			mixed, err := decodeMixed(item)
			if err != nil {
				return errors.Trace(err)
			}
			if mixed.hasLock() {
				lock := mixed.lock
				if lock.startTS == startTS {
					lockKey, err1 := decodeRawKey(item.Key())
					if err1 != nil {
						return errors.Trace(err1)
					}
					lockKeys = append(lockKeys, lockKey)
					lockVers = append(lockVers, item.Version())
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	var tmpDiff int64
	err = updateWithRetry(store.db, func(txn *badger.Txn) error {
		tmpDiff = 0
		for i, lockKey := range lockKeys {
			mvKey := encodeMVKey(lockKey)
			item, err := txn.Get(mvKey)
			if err != nil && err != badger.ErrKeyNotFound {
				return errors.Trace(err)
			}
			if item.Version() != lockVers[i] {
				continue
			}
			if commitTS > 0 {
				err = store.commitKey(txn, lockKey, startTS, commitTS, &tmpDiff)
			} else {
				err = store.rollbackKey(txn, lockKey, startTS)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("resolve lock failed with %d locks, %v", len(lockKeys), err)
		return errors.Trace(err)
	}
	atomic.AddInt64(diff, tmpDiff)
	return nil
}

func (store *MVCCStore) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	return nil
}

const delRangeBatchSize = 4096

func (store *MVCCStore) DeleteRange(startKey, endKey []byte) error {
	keys := make([][]byte, 0, delRangeBatchSize)
	mvStartKey := encodeMVKey(startKey)
	mvEndKey := encodeMVKey(endKey)
	oldStartKey := encodeOldKeyFromMVKey(mvStartKey, lockVer)
	oldEndKey := encodeOldKeyFromMVKey(mvEndKey, lockVer)

	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		keys = store.collectRangeKeys(iter, mvStartKey, mvEndKey, keys)
		keys = store.collectRangeKeys(iter, oldStartKey, oldEndKey, keys)
		return nil
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	err = store.deleteKeysInBatch(keys)
	if err != nil {
		log.Error(err)
	}
	return errors.Trace(err)
}

func (store *MVCCStore) collectRangeKeys(iter *badger.Iterator, mvStartKey, mvEndKey []byte, keys [][]byte) [][]byte {
	for iter.Seek(mvStartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		mvKey := item.KeyCopy(nil)
		if exceedEndKey(mvKey, mvEndKey) {
			break
		}
		keys = append(keys, mvKey)
		if len(keys) == delRangeBatchSize {
			break
		}
	}
	return keys
}

func (store *MVCCStore) deleteKeysInBatch(keys [][]byte) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), delRangeBatchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		err := updateWithRetry(store.db, func(txn *badger.Txn) error {
			for _, key := range batchKeys {
				err1 := txn.Delete(key)
				if err1 != nil {
					return errors.Trace(err1)
				}
			}
			return nil
		})
		if err != nil {
			log.Error(err)
			return errors.Trace(err)
		}
	}
	return nil
}

const gcBatchSize = 256

func (store *MVCCStore) GC(mvStartKey, mvEndKey []byte, safePoint uint64) error {
	// TODO:
	return nil
}

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}
