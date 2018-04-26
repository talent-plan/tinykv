package tikv

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	mu sync.RWMutex
	db *badger.DB
}

func (store *MVCCStore) Get(key []byte, startTS uint64) ([]byte, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var val []byte
	err := store.db.View(func(txn *badger.Txn) error {
		txnIt := store.newIterator(txn)
		var err1 error
		_, val, err1 = store.mvccGet(txnIt, key, startTS)
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

func (store *MVCCStore) mvccGet(txnIt *badger.Iterator, key []byte, startTS uint64) (uint64, []byte, error) {
	mvccSeekKey := mvccEncode(key, startTS)
	txnIt.Seek(mvccSeekKey)
	if !txnIt.Valid() {
		return 0, nil, badger.ErrKeyNotFound
	}
	mvccFoundKey := txnIt.Item().Key()
	foundKey, commitTS, err1 := mvDecode(mvccFoundKey)
	if err1 != nil {
		return 0, nil, errors.Trace(err1)
	}
	if !bytes.Equal(foundKey, key) {
		return 0, nil, badger.ErrKeyNotFound
	}
	val, err := txnIt.Item().Value()
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	var mvVal mvccValue
	err = mvVal.UnmarshalBinary(val)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return commitTS, mvVal.value, err
}

func (store *MVCCStore) BatchGet(ks [][]byte, startTS uint64) []Pair {
	return nil
}

func (store *MVCCStore) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	store.mu.Lock()
	defer store.mu.Unlock()

	anyError := false
	errs := make([]error, 0, len(mutations))
	err := store.db.Update(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		for _, m := range mutations {
			err1 := store.prewriteMutation(txn, iter, m, primary, startTS, ttl)
			errs = append(errs, err1)
			if err1 != nil {
				anyError = true
			}
		}
		return nil
	})
	if err != nil {
		return []error{err}
	}
	return errs
}

const lockVer uint64 = math.MaxUint64

func (store *MVCCStore) prewriteMutation(txn *badger.Txn, iter *badger.Iterator, mutation *kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) error {
	lockKey := mvccEncode(mutation.Key, lockVer)
	err := store.checkPrewriteConflict(iter, lockKey, mutation.Key, startTS)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
		return errors.Trace(err)
	}
	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.Op,
		ttl:     ttl,
	}
	lockVal, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Set(lockKey, lockVal)
}

func (store *MVCCStore) checkPrewriteConflict(iter *badger.Iterator, lockKey, key []byte, startTS uint64) error {
	iter.Seek(lockKey)
	if !iter.Valid() {
		return nil
	}
	foundMvccKey := iter.Item().Key()
	foundKey, ts, err := mvDecode(foundMvccKey)
	if err != nil {
		return errors.Trace(err)
	}
	if !bytes.Equal(foundKey, key) {
		return nil
	}
	if ts < startTS {
		return nil
	}
	if ts != lockVer {
		return ErrRetryable("write conflict")
	}

	// There is a lock, check if its locked by the same transaction.
	var lockVal []byte
	lockVal, err = iter.Item().Value()
	if err != nil {
		return err
	}
	var lock mvccLock
	err = lock.UnmarshalBinary(lockVal)
	if err != nil {
		return err
	}
	if lock.startTS == startTS {
		return nil
	}
	return &ErrLocked{
		Key:     foundMvccKey,
		Primary: lock.primary,
		StartTS: lock.startTS,
		TTL:     lock.ttl,
	}
}

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(keys [][]byte, startTS, commitTS uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	return store.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := store.commitKey(txn, key, startTS, commitTS)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

func (store *MVCCStore) commitKey(txn *badger.Txn, key []byte, startTS, commitTS uint64) error {
	lockKey := mvccEncode(key, lockVer)
	item, err := txn.Get(lockKey)
	if err != nil {
		return errors.Trace(err)
	}
	val, err := item.Value()
	if err != nil {
		return errors.Trace(err)
	}
	var lock mvccLock
	err = lock.UnmarshalBinary(val)
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
		writeKey := mvccEncode(key, commitTS)
		writeValue, err := value.MarshalBinary()
		if err != nil {
			return errors.Trace(err)
		}
		err = txn.Set(writeKey, writeValue)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Delete(lockKey)
}

func (store *MVCCStore) Rollback(keys [][]byte, startTS uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	return store.db.Update(func(txn *badger.Txn) error {
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
}

func (store *MVCCStore) rollbackKey(iter *badger.Iterator, txn *badger.Txn, key []byte, startTS uint64) error {
	lockKey := mvccEncode(key, lockVer)
	iter.Seek(lockKey)
	if iter.Valid() {
		item := iter.Item()
		foundMvccKey := item.Key()
		foundKey, ts, err := mvDecode(foundMvccKey)
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
	writeKey := mvccEncode(key, startTS)
	writeValue, err := tomb.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Set(writeKey, writeValue)
}

func (store *MVCCStore) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var pairs []Pair
	store.db.View(func(txn *badger.Txn) error {
		iter := store.newIterator(txn)
		defer iter.Close()
		seekKey := mvccEncode(startKey, lockVer)
		var mvEndKey []byte
		if endKey != nil {
			mvEndKey = mvccEncode(endKey, lockVer)
		}
		for len(pairs) < limit {
			iter.Seek(seekKey)
			pair := getPairWithStartTS(iter, mvEndKey, startTS)
			if pair.Key == nil {
				return nil
			}
			pairs = append(pairs, pair)
			if pair.Err != nil {
				return nil
			}
			seekKey = mvccEncode(pair.Key, 0)
		}
		return nil
	})
	return pairs
}

// 1. seek with lock key
// 2. found lock, decode it to see if we need to return locked error
func getPairWithStartTS(iter *badger.Iterator, mvEndKey []byte, startTS uint64) (pair Pair) {
	for iter.Valid() {
		item := iter.Item()
		mvFoundKey := item.Key()
		if len(mvEndKey) > 0 && bytes.Compare(mvFoundKey, mvEndKey) >= 0 {
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
				pair.Err = &ErrLocked{
					Key:     mvFoundKey,
					Primary: lock.primary,
					StartTS: lock.startTS,
					TTL:     lock.ttl,
				}
				return
			}
			iter.Next()
			continue
		}
		if !isVisibleKey(item.Key(), startTS) {
			iter.Next()
			continue
		}
		foundKey, _, err := mvDecode(item.Key())
		if err != nil {
			pair.Err = err
			return
		}
		pair.Key = foundKey
		val, err := item.Value()
		if err != nil {
			pair.Err = err
			return
		}
		var mvVal mvccValue
		pair.Err = mvVal.UnmarshalBinary(val)
		pair.Value = mvVal.value
		return
	}
	return
}

func isLockKey(mvKey []byte) bool {
	return binary.BigEndian.Uint64(mvKey[len(mvKey)-8:]) == 0
}

func isVisibleKey(mvKey []byte, startTS uint64) bool {
	ts := ^(binary.BigEndian.Uint64(mvKey[len(mvKey)-8:]))
	return startTS >= ts
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (store *MVCCStore) ReverseScan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var pairs []Pair
	store.db.View(func(txn *badger.Txn) error {
		var opts badger.IteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		seekKey := mvccEncode(endKey, lockVer)
		mvStartKey := mvccEncode(startKey, lockVer)
		for len(pairs) < limit {
			iter.Seek(seekKey)
			pair := getPairWithStartTSReverse(iter, mvStartKey, startTS)
			if pair.Key == nil {
				return nil
			}
			pairs = append(pairs, pair)
			if pair.Err != nil {
				return nil
			}
			seekKey = mvccEncode(pair.Key, lockVer)
		}
		return nil
	})
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
				pair.Err = &ErrLocked{
					Key:     mvFoundKey,
					Primary: lock.primary,
					StartTS: lock.startTS,
					TTL:     lock.ttl,
				}
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
		foundKey, _, err := mvDecode(keyBuf)
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
	return nil
}

func (store *MVCCStore) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	return nil, nil
}

func (store *MVCCStore) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	return nil
}

func (store *MVCCStore) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	return nil
}

func (store *MVCCStore) DeleteRange(startKey, endKey []byte) error {
	return nil
}

// mvccEncode returns the encoded key.
func mvccEncode(key []byte, ver uint64) []byte {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver)
	return ret
}

// mvDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func mvDecode(encodedKey []byte) ([]byte, uint64, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes(encodedKey, nil)
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
func (v mvccValue) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, int64(v.valueType))
	mh.WriteNumber(&buf, v.startTS)
	mh.WriteNumber(&buf, v.commitTS)
	mh.WriteSlice(&buf, v.value)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	var vt int64
	mh.ReadNumber(buf, &vt)
	v.valueType = mvccValueType(vt)
	mh.ReadNumber(buf, &v.startTS)
	mh.ReadNumber(buf, &v.commitTS)
	mh.ReadSlice(buf, &v.value)
	return errors.Trace(mh.err)
}

type mvccLock struct {
	startTS uint64
	primary []byte
	value   []byte
	op      kvrpcpb.Op
	ttl     uint64
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, l.startTS)
	mh.WriteSlice(&buf, l.primary)
	mh.WriteSlice(&buf, l.value)
	mh.WriteNumber(&buf, l.op)
	mh.WriteNumber(&buf, l.ttl)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	mh.ReadNumber(buf, &l.startTS)
	mh.ReadSlice(buf, &l.primary)
	mh.ReadSlice(buf, &l.value)
	mh.ReadNumber(buf, &l.op)
	mh.ReadNumber(buf, &l.ttl)
	return errors.Trace(mh.err)
}

type marshalHelper struct {
	err error
}

func (mh *marshalHelper) ReadNumber(r io.Reader, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Read(r, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) ReadSlice(r *bytes.Buffer, slice *[]byte) {
	if mh.err != nil {
		return
	}
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		mh.err = errors.Trace(err)
		return
	}
	const c10M = 10 * 1024 * 1024
	if sz > c10M {
		mh.err = errors.New("too large slice, maybe something wrong")
		return
	}
	data := make([]byte, sz)
	if _, err := io.ReadFull(r, data); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	*slice = data
}

func (mh *marshalHelper) WriteSlice(buf io.Writer, slice []byte) {
	if mh.err != nil {
		return
	}
	var tmp [binary.MaxVarintLen64]byte
	off := binary.PutUvarint(tmp[:], uint64(len(slice)))
	if err := writeFull(buf, tmp[:off]); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	if err := writeFull(buf, slice); err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) WriteNumber(buf io.Writer, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func writeFull(w io.Writer, slice []byte) error {
	written := 0
	for written < len(slice) {
		n, err := w.Write(slice[written:])
		if err != nil {
			return errors.Trace(err)
		}
		written += n
	}
	return nil
}

// MvccKey is the encoded key type.
// On TiKV, keys are encoded before they are saved into storage engine.
type MvccKey []byte

// NewMvccKey encodes a key into MvccKey.
func NewMvccKey(key []byte) MvccKey {
	if len(key) == 0 {
		return nil
	}
	return codec.EncodeBytes(nil, key)
}

// Raw decodes a MvccKey to original key.
func (key MvccKey) Raw() []byte {
	if len(key) == 0 {
		return nil
	}
	_, k, err := codec.DecodeBytes(key, nil)
	if err != nil {
		panic(err)
	}
	return k
}

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}
