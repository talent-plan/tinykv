package tikv

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync/atomic"
	"time"

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
	var val []byte
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		var err1 error
		_, val, err1 = store.mvGet(iter, key, startTS)
		if err1 == badger.ErrKeyNotFound {
			err1 = nil
		}
		return err1
	})
	return val, err
}

func (store *MVCCStore) newIterator(txn *badger.Txn) *badger.Iterator {
	var itOpts = badger.DefaultIteratorOptions
	itOpts.PrefetchValues = false
	return txn.NewIterator(itOpts)
}

func (store *MVCCStore) mvGet(txnIt *badger.Iterator, key []byte, startTS uint64) (uint64, []byte, error) {
	mvSeekKey := mvEncode(key, startTS)
	txnIt.Seek(mvSeekKey)
	for txnIt.Valid() {
		item := txnIt.Item()
		mvFoundKey := item.Key()
		if !equalRawKey(mvFoundKey, mvSeekKey) {
			break
		}
		if isLockKey(mvFoundKey) {
			lock, err := decodeLock(item)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			if lock.startTS > startTS {
				txnIt.Next()
				continue
			}
			return 0, nil, lock.toError(mvFoundKey)
		}
		if !isVisibleKey(mvFoundKey, startTS) {
			txnIt.Next()
			continue
		}
		mvVal, err1 := decodeValue(item)
		if err1 != nil {
			return 0, nil, errors.Trace(err1)
		}
		if mvVal.valueType == typeRollback {
			txnIt.Next()
			continue
		} else if mvVal.valueType == typeDelete {
			break
		}
		return mvVal.commitTS, mvVal.value, nil
	}
	return 0, nil, badger.ErrKeyNotFound
}

func equalRawKey(mvFoundKey, mvSeekKey []byte) bool {
	lenFoundKey := len(mvFoundKey) - 8
	if lenFoundKey <= 0 {
		return false
	}
	return bytes.Equal(mvFoundKey[:lenFoundKey], mvSeekKey[:len(mvSeekKey)-8])
}

func decodeValue(item *badger.Item) (mvVal mvccValue, err error) {
	val, err := item.Value()
	if err != nil {
		return mvVal, errors.Trace(err)
	}
	err = mvVal.UnmarshalBinary(val)
	if err != nil {
		return mvVal, errors.Trace(err)
	}
	return mvVal, nil
}

func decodeLock(item *badger.Item) (mvccLock, error) {
	var lock mvccLock
	lockVal, err := item.Value()
	if err != nil {
		return lock, err
	}
	err = lock.UnmarshalBinary(lockVal)
	return lock, err
}

func (store *MVCCStore) BatchGet(keys [][]byte, startTS uint64) []Pair {
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for _, key := range keys {
			_, val, err1 := store.mvGet(iter, key, startTS)
			if err1 == badger.ErrKeyNotFound {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Value: val, Err: err1})
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
		iter := store.newIterator(txn)
		defer iter.Close()
		for _, m := range mutations {
			err1 := store.prewriteMutation(txn, iter, m, primary, startTS, ttl)
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

func (store *MVCCStore) prewriteMutation(txn *badger.Txn, iter *badger.Iterator, mutation *kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) error {
	mvLockKey := mvEncode(mutation.Key, lockVer)
	err := store.checkPrewriteConflict(iter, mvLockKey, startTS)
	if err != nil {
		return errors.Trace(err)
	}
	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.Op,
		ttl:     ttl,
	}
	lockVal := lock.MarshalBinary()
	return txn.Set(mvLockKey, lockVal)
}

func (store *MVCCStore) checkPrewriteConflict(iter *badger.Iterator, mvLockKey []byte, startTS uint64) error {
	iter.Seek(mvLockKey)
	prefix := mvLockKey[:len(mvLockKey)-8]
	for iter.ValidForPrefix(prefix) {
		item := iter.Item()
		if !equalRawKey(item.Key(), mvLockKey) {
			return nil
		}
		if isLockKey(item.Key()) {
			lock, err := decodeLock(item)
			if err != nil {
				return errors.Trace(err)
			}
			if lock.startTS == startTS {
				return nil
			}
			log.Debugf("lock err %x", item.Key())
			return lock.toError(item.Key())
		}
		if isVisibleKey(item.Key(), startTS) {
			return nil
		}
		mvVal, err := decodeValue(item)
		if err != nil {
			return errors.Trace(err)
		}
		if mvVal.valueType != typeRollback {
			log.Debugf("write conflict %x", item.Key())
			return ErrRetryable("write conflict")
		}
		iter.Next()
	}
	return nil
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
	lockKey := mvEncode(key, lockVer)
	item, err := txn.Get(lockKey)
	if err != nil {
		return errors.Trace(err)
	}
	lock, err := decodeLock(item)
	if err != nil {
		return errors.Trace(err)
	}
	if lock.startTS != startTS {
		if lock.op != kvrpcpb.Op_Rollback {
			return errors.New("replaced by another transaction")
		}
	} else {
		if lock.op == kvrpcpb.Op_Rollback {
			return errors.New("already rollback")
		}
	}
	if lock.op != kvrpcpb.Op_Lock {
		var valueType mvccValueType
		if lock.op == kvrpcpb.Op_Put {
			valueType = typePut
		} else {
			valueType = typeDelete
		}
		value := mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     lock.value,
		}
		writeKey := mvEncode(key, commitTS)
		writeValue := value.MarshalBinary()
		*diff += int64(len(writeKey) + len(writeValue))
		err = txn.Set(writeKey, writeValue)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Delete(lockKey)
}

func (store *MVCCStore) Rollback(keys [][]byte, startTS uint64) error {
	err1 := updateWithRetry(store.db, func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for _, key := range keys {
			err := store.rollbackKey(iter, txn, key, startTS)
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

func (store *MVCCStore) rollbackKey(iter *badger.Iterator, txn *badger.Txn, key []byte, startTS uint64) error {
	lockKey := mvEncode(key, lockVer)
	iter.Seek(lockKey)
	if iter.Valid() {
		item := iter.Item()
		foundMvccKey := item.Key()
		var keyBuf [64]byte
		foundKey, ts, err := mvDecode(foundMvccKey, keyBuf[:])
		if err != nil {
			return errors.Trace(err)
		}
		if bytes.Equal(foundKey, key) {
			if ts == lockVer {
				var lockVal []byte
				lockVal, err = item.Value()
				if err != nil {
					return errors.Trace(err)
				}
				var lock mvccLock
				err = lock.UnmarshalBinary(lockVal)
				if err != nil {
					return errors.Trace(err)
				}
				if lock.startTS == startTS {
					err = txn.Delete(lockKey)
					if err != nil {
						return errors.Trace(err)
					}
				}
			} else if ts >= startTS {
				// Already committed or rollbacked.
				return nil
			}
		}
	}
	tomb := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvEncode(key, startTS)
	writeValue := tomb.MarshalBinary()
	return txn.Set(writeKey, writeValue)
}

func (store *MVCCStore) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		var mvEndKey []byte
		if endKey != nil {
			mvEndKey = mvEncode(endKey, lockVer)
		}
		iter.Seek(mvEncode(startKey, lockVer))
		for len(pairs) < limit {
			pair := getPairWithStartTS(iter, mvEndKey, startTS)
			if pair.Err != nil {
				return pair.Err
			}
			if pair.Key == nil {
				return nil
			}
			pairs = append(pairs, pair)
			nearSeek(iter, mvEncode(pair.Key, 0))
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return pairs
}

func nearSeek(iter *badger.Iterator, mvSeekKey []byte) {
	cnt := 0
	for iter.Valid() {
		item := iter.Item()
		if reachBound(item.Key(), mvSeekKey) {
			return
		}
		if cnt > 16 {
			iter.Seek(mvSeekKey)
			return
		}
		cnt++
		iter.Next()
	}
}

// 1. seek with lock key
// 2. found lock, decode it to see if we need to return locked error
func getPairWithStartTS(iter *badger.Iterator, mvEndKey []byte, startTS uint64) (pair Pair) {
	for iter.Valid() {
		item := iter.Item()
		mvFoundKey := item.Key()
		if reachBound(mvFoundKey, mvEndKey) {
			return
		}
		if isLockKey(mvFoundKey) {
			lock, err := decodeLock(item)
			if err != nil {
				pair.Err = err
				return
			}
			if lock.startTS < startTS {
				pair.Err = lock.toError(mvFoundKey)
				return
			}
			iter.Next()
			continue
		}
		if !isVisibleKey(item.Key(), startTS) {
			iter.Next()
			continue
		}
		var keyBuf [64]byte
		foundKey, _, err := mvDecode(item.Key(), keyBuf[:])
		if err != nil {
			pair.Err = err
			return
		}
		mvVal, err := decodeValue(item)
		if err != nil {
			pair.Err = err
			return
		}
		if mvVal.valueType == typeRollback || mvVal.valueType == typeDelete {
			nearSeek(iter, mvEncode(foundKey, 0))
			continue
		}
		pair.Key = append(pair.Key[:0], foundKey...)
		pair.Value = mvVal.value
		return
	}
	return
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
		seekKey := mvEncode(endKey, lockVer)
		mvStartKey := mvEncode(startKey, lockVer)
		for len(pairs) < limit {
			iter.Seek(seekKey)
			pair := getPairWithStartTSReverse(iter, mvStartKey, startTS)
			if pair.Err != nil {
				return pair.Err
			}
			if pair.Key == nil {
				return nil
			}
			pairs = append(pairs, pair)
			seekKey = mvEncode(pair.Key, lockVer)
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return nil
}

func getPairWithStartTSReverse(iter *badger.Iterator, mvStartKey []byte, startTS uint64) (pair Pair) {
	var keyBuf, valBuf []byte
	for iter.Valid() {
		item := iter.Item()
		mvFoundKey := item.Key()
		if bytes.Compare(mvFoundKey, mvStartKey) < 0 {
			return
		}
		if isLockKey(mvFoundKey) {
			var lock mvccLock
			val, err1 := item.Value()
			if err1 != nil {
				pair.Err = err1
				return
			}
			err1 = lock.UnmarshalBinary(val)
			if err1 != nil {
				pair.Err = err1
				return
			}
			if lock.startTS < startTS {
				pair.Err = lock.toError(mvFoundKey)
				return
			}
			iter.Next()
			if len(keyBuf) > 0 {
				break
			}
			continue
		}
		if isVisibleKey(item.Key(), startTS) {
			keyBuf = item.KeyCopy(keyBuf)
			var err error
			valBuf, err = item.ValueCopy(valBuf)
			if err != nil {
				pair.Err = err
				return
			}
			iter.Next()
			continue
		}
		iter.Next()
		if len(keyBuf) > 0 {
			break
		}
	}
	if len(keyBuf) != 0 {
		foundKey, _, err := mvDecode(keyBuf, nil)
		if err != nil {
			pair.Err = err
			return
		}
		pair.Key = foundKey
		var mvVal mvccValue
		pair.Err = mvVal.UnmarshalBinary(valBuf)
		pair.Value = mvVal.value
	}
	return
}

func (store *MVCCStore) Cleanup(key []byte, startTS uint64) error {
	err := updateWithRetry(store.db, func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		return store.rollbackKey(iter, txn, key, startTS)
	})
	if err != nil {
		log.Error(err)
	}
	return err
}

func (store *MVCCStore) ScanLock(mvStartKey, mvEndKey []byte, limit int, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	err1 := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		iter.Seek(mvStartKey)
		for {
			lockInfo, err := store.scanOneLock(iter, mvEndKey, maxTS)
			if err != nil {
				return errors.Trace(err)
			}
			if lockInfo == nil {
				return nil
			}
			locks = append(locks, lockInfo)
			if len(locks) == limit {
				break
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error(err1)
	}
	return nil, nil
}

func (store *MVCCStore) scanOneLock(iter *badger.Iterator, mvEndKey []byte, maxTS uint64) (*kvrpcpb.LockInfo, error) {
	for iter.Valid() {
		item := iter.Item()
		if reachBound(item.Key(), mvEndKey) {
			return nil, nil
		}
		if isLockKey(item.Key()) {
			val, err := item.Value()
			if err != nil {
				return nil, errors.Trace(err)
			}
			var lock mvccLock
			err = lock.UnmarshalBinary(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if lock.startTS < maxTS {
				iter.Next()
				return &kvrpcpb.LockInfo{
					PrimaryLock: lock.primary,
					LockVersion: lock.startTS,
					Key:         item.KeyCopy(nil),
					LockTtl:     lock.ttl,
				}, nil
			}
		}
		iter.Next()
	}
	return nil, nil
}

func (store *MVCCStore) ResolveLock(mvStartKey, mvEndKey []byte, startTS, commitTS uint64, diff *int64) error {
	var lockKeys [][]byte
	var lockValues []mvccLock
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for iter.Seek(mvStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if reachBound(item.Key(), mvEndKey) {
				return nil
			}
			if isLockKey(item.Key()) {
				lock, err := decodeLock(item)
				if err != nil {
					return errors.Trace(err)
				}
				if lock.startTS == startTS {
					lockKeys = append(lockKeys, item.KeyCopy([]byte{}))
					lockValues = append(lockValues, lock)
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
			var err error
			if commitTS > 0 {
				err = store.commitLock(txn, lockValues[i], lockKey, startTS, commitTS, &tmpDiff)
			} else {
				err = store.rollbackLock(txn, lockValues[i], lockKey, startTS)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	atomic.AddInt64(diff, tmpDiff)
	return nil
}

func (store *MVCCStore) commitLock(txn *badger.Txn, lock mvccLock, mvLockKey []byte, startTS, commitTS uint64, diff *int64) error {
	if lock.op != kvrpcpb.Op_Lock {
		var valueType mvccValueType
		if lock.op == kvrpcpb.Op_Put {
			valueType = typePut
		} else {
			valueType = typeDelete
		}
		value := mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     lock.value,
		}
		var buf [64]byte
		writeKey := append(buf[:0], mvLockKey...)
		binary.BigEndian.PutUint64(writeKey[len(writeKey)-8:], ^commitTS)
		writeValue := value.MarshalBinary()
		*diff += int64(len(writeKey) + len(writeValue))
		err := txn.Set(writeKey, writeValue)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Delete(mvLockKey)
}

func (store *MVCCStore) rollbackLock(txn *badger.Txn, lock mvccLock, mvLockKey []byte, startTS uint64) error {
	tomb := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	var writeKey []byte
	writeKey = append(writeKey, mvLockKey...)
	binary.BigEndian.PutUint64(writeKey[len(writeKey)-8:], ^startTS)
	writeValue := tomb.MarshalBinary()
	err := txn.Set(writeKey, writeValue)
	if err != nil {
		return errors.Trace(err)
	}
	var delKey []byte
	delKey = append(delKey, mvLockKey...)
	return txn.Delete(delKey)
}

func (store *MVCCStore) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	return nil
}

const delRangeBatchSize = 256

func (store *MVCCStore) DeleteRange(startKey, endKey []byte) error {
	keys := make([][]byte, 0, delRangeBatchSize)
	mvSeekKey := mvEncode(startKey, lockVer)
	mvEndKey := mvEncode(endKey, lockVer)
	for {
		err := store.db.View(func(txn *badger.Txn) error {
			iter := store.newIterator(txn)
			for iter.Seek(mvSeekKey); iter.Valid(); iter.Next() {
				item := iter.Item()
				mvKey := item.KeyCopy(nil)
				mvSeekKey = mvKey
				if reachBound(mvKey, mvEndKey) {
					break
				}
				keys = append(keys, mvKey)
				if len(keys) == delRangeBatchSize {
					break
				}
			}
			return nil
		})
		if err != nil {
			log.Error(err)
			return errors.Trace(err)
		}
		if len(keys) == 0 {
			return nil
		}
		err = updateWithRetry(store.db, func(txn *badger.Txn) error {
			for _, key := range keys {
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
		log.Debug("delete range", len(keys), keys[0])
		keys = keys[:0]
	}
}

const gcBatchSize = 256

func (store *MVCCStore) GC(mvStartKey, mvEndKey []byte, safePoint uint64) error {
	var allGCKeys [][]byte
	mvSeekKey := mvStartKey
	err := store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		for iter.Seek(mvSeekKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if reachBound(item.Key(), mvEndKey) {
				break
			}
			if bytes.HasPrefix(item.Key(), InternalKeyPrefix) {
				// Internal Keys doesn't has multi-version.
				continue
			}
			gcKeys, err1 := store.allGCVersionsForKey(iter, safePoint)
			if err1 != nil {
				return errors.Trace(err1)
			}
			allGCKeys = append(allGCKeys, gcKeys...)
			if !iter.Valid() {
				break
			}
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	if len(allGCKeys) == 0 {
		return nil
	}
	for len(allGCKeys) > 0 {
		batchSize := mathutil.Min(len(allGCKeys), gcBatchSize)
		gcBatchKeys := allGCKeys[:batchSize]
		allGCKeys = allGCKeys[batchSize:]
		err = updateWithRetry(store.db, func(txn *badger.Txn) error {
			for _, gcKey := range gcBatchKeys {
				err1 := txn.Delete(gcKey)
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

func (store *MVCCStore) allGCVersionsForKey(iter *badger.Iterator, safePoint uint64) ([][]byte, error) {
	var gcKeys [][]byte
	var keyBuf [128]byte
	key := iter.Item().KeyCopy(keyBuf[:])
	keyPrefix := key[:len(key)-8]
	var hasValidKey bool
	for ; iter.ValidForPrefix(keyPrefix); iter.Next() {
		item := iter.Item()
		if isLockKey(item.Key()) {
			continue
		}
		if hasValidKey {
			if isVisibleKey(item.Key(), safePoint) {
				log.Debugf("gc key %x", item.Key())
				gcKeys = append(gcKeys, item.KeyCopy(nil))
			}
		} else {
			mvVal, err := decodeValue(item)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if mvVal.valueType != typeRollback {
				hasValidKey = true
			}
		}
	}
	return gcKeys, nil
}

// mvEncode returns the encoded key.
func mvEncode(key []byte, ver uint64) []byte {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver)
	return ret
}

// mvDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func mvDecode(encodedKey []byte, buf []byte) ([]byte, uint64, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes(encodedKey, buf)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	// if it's meta key
	if len(remainBytes) == 0 {
		return key, 0, nil
	}
	var ver uint64
	remainBytes, ver, err = codec.DecodeUintDesc(remainBytes)
	if err != nil {
		// should never happen
		return nil, 0, errors.Trace(err)
	}
	if len(remainBytes) != 0 {
		return nil, 0, errors.New("invalid encode key")
	}
	return key, ver, nil
}

type mvccValueType int

const (
	typePut mvccValueType = iota
	typeDelete
	typeRollback
)

type mvccValue struct {
	valueType mvccValueType
	startTS   uint64
	commitTS  uint64
	value     []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() []byte {
	buf := make([]byte, 0, len(v.value)+32)
	buf = codec.EncodeUint(buf, uint64(v.valueType))
	buf = codec.EncodeUint(buf, v.startTS)
	buf = codec.EncodeUint(buf, v.commitTS)
	buf = codec.EncodeCompactBytes(buf, v.value)
	return buf
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) error {
	data, uVal, err := codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	v.valueType = mvccValueType(uVal)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	v.startTS = uVal
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	v.commitTS = uVal
	data, bVal, err := codec.DecodeCompactBytes(data)
	if err != nil {
		return errors.Trace(err)
	}
	v.value = append([]byte{}, bVal...)
	return nil
}

type mvccLock struct {
	startTS uint64
	primary []byte
	value   []byte
	op      kvrpcpb.Op
	ttl     uint64
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() []byte {
	buf := make([]byte, 0, len(l.primary)+len(l.value)+32)
	buf = codec.EncodeUint(buf, l.startTS)
	buf = codec.EncodeCompactBytes(buf, l.primary)
	buf = codec.EncodeCompactBytes(buf, l.value)
	buf = codec.EncodeUint(buf, uint64(l.op))
	buf = codec.EncodeUint(buf, l.ttl)
	return buf
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) error {
	data, uVal, err := codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	l.startTS = uVal
	data, bVal, err := codec.DecodeCompactBytes(data)
	if err != nil {
		return errors.Trace(err)
	}
	l.primary = append([]byte{}, bVal...)
	data, bVal, err = codec.DecodeCompactBytes(data)
	if err != nil {
		return errors.Trace(err)
	}
	l.value = append([]byte{}, bVal...)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	l.op = kvrpcpb.Op(uVal)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return errors.Trace(err)
	}
	l.ttl = uVal
	return nil
}

func (l *mvccLock) toError(mvKey []byte) *ErrLocked {
	rawKey, _, err := mvDecode(mvKey, nil)
	if err != nil {
		log.Error(err)
	}
	return &ErrLocked{
		Key:     rawKey,
		StartTS: l.startTS,
		Primary: l.primary,
		TTL:     l.ttl,
	}
}

func extractPhysicalTime(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.Unix(t/1e3, (t%1e3)*1e6)
}

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}
