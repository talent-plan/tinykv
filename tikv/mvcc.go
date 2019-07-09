package tikv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	dir           string
	db            *badger.DB
	lockStore     *lockstore.MemStore
	rollbackStore *lockstore.MemStore
	dbWriter      mvcc.DBWriter

	safePoint *SafePoint
	gcLock    sync.Mutex
}

// NewMVCCStore creates a new MVCCStore
func NewMVCCStore(bundle *mvcc.DBBundle, dataDir string, safePoint *SafePoint, writer mvcc.DBWriter) *MVCCStore {
	store := &MVCCStore{
		db:            bundle.DB,
		dir:           dataDir,
		lockStore:     bundle.LockStore,
		rollbackStore: bundle.RollbackStore,
		safePoint:     safePoint,
		dbWriter:      writer,
	}
	err := store.loadLocks()
	if err != nil {
		log.Fatal(err)
	}
	store.loadSafePoint()
	writer.Open()
	return store
}

func (store *MVCCStore) loadLocks() error {
	fileName := store.dir + "/lock_store"
	f, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Trace(err)
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	hdrBuf := make([]byte, 8)
	hdr := (*lockEntryHdr)(unsafe.Pointer(&hdrBuf[0]))
	var keyBuf, valBuf []byte
	for {
		_, err = reader.Read(hdrBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		if cap(keyBuf) < int(hdr.keyLen) {
			keyBuf = make([]byte, hdr.keyLen)
		}
		if cap(valBuf) < int(hdr.valLen) {
			valBuf = make([]byte, hdr.valLen)
		}
		keyBuf = keyBuf[:hdr.keyLen]
		valBuf = valBuf[:hdr.valLen]
		_, err = reader.Read(keyBuf)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = reader.Read(valBuf)
		if err != nil {
			return errors.Trace(err)
		}
		store.lockStore.Insert(keyBuf, valBuf)
	}
	return os.Remove(fileName)
}

func (store *MVCCStore) loadSafePoint() {
	err := store.db.View(func(txn *badger.Txn) error {
		item, err1 := txn.Get(InternalSafePointKey)
		if err1 == badger.ErrKeyNotFound {
			return nil
		} else if err1 != nil {
			return err1
		}
		val, err1 := item.Value()
		if err1 != nil {
			return err1
		}
		atomic.StoreUint64(&store.safePoint.timestamp, binary.LittleEndian.Uint64(val))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func (store *MVCCStore) Close() error {
	store.dbWriter.Close()

	err := store.dumpMemLocks()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

type lockEntryHdr struct {
	keyLen uint32
	valLen uint32
}

func (store *MVCCStore) dumpMemLocks() error {
	tmpFileName := store.dir + "/lock_store.tmp"
	f, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return errors.Trace(err)
	}
	writer := bufio.NewWriter(f)
	cnt := 0
	it := store.lockStore.NewIterator()
	hdrBuf := make([]byte, 8)
	hdr := (*lockEntryHdr)(unsafe.Pointer(&hdrBuf[0]))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		hdr.keyLen = uint32(len(it.Key()))
		hdr.valLen = uint32(len(it.Value()))
		writer.Write(hdrBuf)
		writer.Write(it.Key())
		writer.Write(it.Value())
		cnt++
	}
	err = writer.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Sync()
	if err != nil {
		return errors.Trace(err)
	}
	f.Close()
	return os.Rename(tmpFileName, store.dir+"/lock_store")
}

func (store *MVCCStore) Prewrite(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	errs := make([]error, 0, len(mutations))
	anyError := false

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	// Must check the LockStore first.
	for _, m := range mutations {
		duplicate, err := store.checkPrewriteInLockStore(reqCtx, m, startTS)
		if err != nil {
			anyError = true
		}
		if duplicate {
			return nil
		}
		errs = append(errs, err)
	}
	if anyError {
		return errs
	}

	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	// Check the DB.
	txn := reqCtx.getDBReader().GetTxn()
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}
	items, err := txn.MultiGet(keys)
	if err != nil {
		return []error{err}
	}
	var buf []byte
	var enc rowcodec.Encoder
	for i, m := range mutations {
		oldMeta, oldVal, err := store.checkPrewriteInDB(reqCtx, txn, items[i], startTS)
		if err != nil {
			anyError = true
		}
		errs[i] = err
		if !anyError {
			op := m.Op
			if op == kvrpcpb.Op_Insert {
				if len(oldVal) > 0 {
					return []error{&ErrKeyAlreadyExists{Key: m.Key}}
				}
				op = kvrpcpb.Op_Put
			} else if op == kvrpcpb.Op_Lock {
				oldMeta, oldVal = nil, nil
			}
			if rowcodec.IsRowKey(m.Key) && op == kvrpcpb.Op_Put {
				buf, err = enc.EncodeFromOldRow(m.Value, buf)
				if err != nil {
					log.Errorf("err:%v m.Value:%v m.Key:%q m.Op:%d", err, m.Value, m.Key, m.Op)
					return []error{err}
				}
				m.Value = buf
			}
			lock := mvcc.MvccLock{
				MvccLockHdr: mvcc.MvccLockHdr{
					StartTS:    startTS,
					Op:         uint8(op),
					HasOldVer:  oldMeta != nil,
					TTL:        uint32(ttl),
					PrimaryLen: uint16(len(primary)),
				},
				Primary: primary,
				Value:   m.Value,
				OldMeta: oldMeta,
				OldVal:  oldVal,
			}
			batch.Prewrite(m.Key, &lock)
		}
	}
	if anyError {
		return errs
	}
	err = store.dbWriter.Write(batch)
	if err != nil {
		return []error{err}
	}
	return nil
}

func (store *MVCCStore) checkPrewriteInLockStore(
	req *requestCtx, mutation *kvrpcpb.Mutation, startTS uint64) (duplicate bool, err error) {
	req.buf = mvcc.EncodeRollbackKey(req.buf, mutation.Key, startTS)
	if len(store.rollbackStore.Get(req.buf, nil)) > 0 {
		return false, ErrAlreadyRollback
	}
	req.buf = store.lockStore.Get(mutation.Key, req.buf)
	if len(req.buf) == 0 {
		return false, nil
	}
	lock := mvcc.DecodeLock(req.buf)
	if lock.StartTS == startTS {
		// Same ts, no need to overwrite.
		return true, nil
	}
	return false, &ErrLocked{
		Key:     mutation.Key,
		StartTS: lock.StartTS,
		Primary: lock.Primary,
		TTL:     uint64(lock.TTL),
	}
}

// checkPrewrietInDB checks that there is no committed version greater than startTS or return write conflict error.
// And it returns the old version if there is one.
func (store *MVCCStore) checkPrewriteInDB(
	req *requestCtx, txn *badger.Txn, item *badger.Item, startTS uint64) (userMeta mvcc.DBUserMeta, val []byte, err error) {
	if item == nil {
		return nil, nil, nil
	}
	userMeta = mvcc.DBUserMeta(item.UserMeta())
	if userMeta.CommitTS() > startTS {
		return nil, nil, ErrRetryable("write conflict")
	}
	val, err = item.Value()
	if err != nil {
		return nil, nil, err
	}
	return userMeta, val, nil
}

const maxSystemTS uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, req.rpcCtx)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			// We never commit partial keys in Commit request, so if one lock is not found,
			// the others keys must not be found too.
			return store.handleLockNotFound(req, key, startTS, commitTS)
		}
		lock := mvcc.DecodeLock(buf)
		if lock.StartTS != startTS {
			return ErrReplaced
		}
		tmpDiff += len(key) + len(lock.Value)
		batch.Commit(key, &lock)
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) handleLockNotFound(reqCtx *requestCtx, key []byte, startTS, commitTS uint64) error {
	txn := reqCtx.getDBReader().GetTxn()
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		return ErrLockNotFound
	}
	useMeta := mvcc.DBUserMeta(item.UserMeta())
	if useMeta.StartTS() == startTS {
		// Already committed.
		return nil
	} else {
		// The transaction may be committed and moved to old data, we need to look for that.
		oldKey := mvcc.EncodeOldKey(key, commitTS)
		_, err = txn.Get(oldKey)
		if err == nil {
			// Found committed key.
			return nil
		}
	}
	return ErrLockNotFound
}

const (
	rollbackStatusDone    = 0
	rollbackStatusNoLock  = 1
	rollbackStatusNewLock = 2
)

func (store *MVCCStore) Rollback(reqCtx *requestCtx, keys [][]byte, startTS uint64) error {
	hashVals := keysToHashVals(keys...)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	statuses := make([]int, len(keys))
	for i, key := range keys {
		statuses[i] = store.rollbackKeyReadLock(reqCtx, batch, key, startTS)
	}
	for i, key := range keys {
		if statuses[i] == rollbackStatusDone {
			continue
		}
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, statuses[i] == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	err := store.dbWriter.Write(batch)
	return errors.Trace(err)
}

func (store *MVCCStore) rollbackKeyReadLock(reqCtx *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64) (status int) {
	reqCtx.buf = mvcc.EncodeRollbackKey(reqCtx.buf, key, startTS)
	rollbackKey := safeCopy(reqCtx.buf)
	reqCtx.buf = store.rollbackStore.Get(rollbackKey, reqCtx.buf)
	if len(reqCtx.buf) != 0 {
		// Already rollback.
		return rollbackStatusDone
	}
	reqCtx.buf = store.lockStore.Get(key, reqCtx.buf)
	hasLock := len(reqCtx.buf) > 0
	if hasLock {
		lock := mvcc.DecodeLock(reqCtx.buf)
		if lock.StartTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.Rollback(key, false)
			return rollbackStatusDone
		}
		if lock.StartTS == startTS {
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.Rollback(key, true)
			return rollbackStatusDone
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
		return rollbackStatusNewLock
	}
	return rollbackStatusNoLock
}

func (store *MVCCStore) rollbackKeyReadDB(req *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64, hasLock bool) error {
	reader := req.getDBReader()
	item, err := reader.GetTxn().Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	hasVal := item != nil
	if !hasVal && !hasLock {
		// The prewrite request is not arrived, we write a rollback lock to prevent the future prewrite.
		batch.Rollback(key, false)
		return nil
	}

	if !hasVal {
		// Not committed.
		return nil
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		return ErrAlreadyCommitted(userMeta.CommitTS())
	}
	if userMeta.StartTS() < startTS && !hasLock {
		// Prewrite and commit have not arrived.
		batch.Rollback(key, false)
		return nil
	}
	// val.startTS > startTS, look for the key in the old version to check if the key is committed.
	it := reader.GetOldIter()
	oldKey := mvcc.EncodeOldKey(key, userMeta.CommitTS())
	// find greater commit version.
	for it.Seek(oldKey); it.ValidForPrefix(oldKey[:len(oldKey)-8]); it.Next() {
		item := it.Item()
		foundKey := item.Key()
		if isVisibleKey(foundKey, startTS) {
			break
		}
		_, ts, err := codec.DecodeUintDesc(foundKey[len(foundKey)-8:])
		if err != nil {
			return errors.Trace(err)
		}
		if mvcc.OldUserMeta(item.UserMeta()).StartTS() == startTS {
			return ErrAlreadyCommitted(ts)
		}
	}
	return nil
}

func isVisibleKey(key []byte, startTS uint64) bool {
	ts := ^(binary.BigEndian.Uint64(key[len(key)-8:]))
	return startTS >= ts
}

func checkLock(lock mvcc.MvccLock, key []byte, startTS uint64) error {
	lockVisible := lock.StartTS < startTS
	isWriteLock := lock.Op == uint8(kvrpcpb.Op_Put) || lock.Op == uint8(kvrpcpb.Op_Del)
	isPrimaryGet := startTS == maxSystemTS && bytes.Equal(lock.Primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return &ErrLocked{
			Key:     key,
			StartTS: lock.StartTS,
			Primary: lock.Primary,
			TTL:     uint64(lock.TTL),
		}
	}
	return nil
}

func (store *MVCCStore) CheckKeysLock(startTS uint64, keys ...[]byte) error {
	var buf []byte
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		err := checkLock(lock, key, startTS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MVCCStore) CheckRangeLock(startTS uint64, startKey, endKey []byte) error {
	if len(endKey) == 0 {
		panic("invalid end key")
	}

	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		err := checkLock(lock, it.Key(), startTS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MVCCStore) Cleanup(reqCtx *requestCtx, key []byte, startTS uint64) error {
	hashVals := keysToHashVals(key)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	status := store.rollbackKeyReadLock(reqCtx, batch, key, startTS)
	if status != rollbackStatusDone {
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, status == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) ScanLock(reqCtx *requestCtx, maxSystemTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	if len(reqCtx.regCtx.endKey) == 0 {
		panic("invalid end key")
	}

	it := store.lockStore.NewIterator()
	for it.Seek(reqCtx.regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), reqCtx.regCtx.endKey) {
			return locks, nil
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.StartTS < maxSystemTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.StartTS,
				Key:         codec.EncodeBytes(nil, it.Key()),
				LockTtl:     uint64(lock.TTL),
			})
		}
	}
	return locks, nil
}

func (store *MVCCStore) ResolveLock(reqCtx *requestCtx, startTS, commitTS uint64) error {
	regCtx := reqCtx.regCtx
	if len(regCtx.endKey) == 0 {
		panic("invalid end key")
	}
	var lockKeys [][]byte
	var lockVals [][]byte
	it := store.lockStore.NewIterator()
	for it.Seek(regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), regCtx.endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.StartTS != startTS {
			continue
		}
		lockKeys = append(lockKeys, safeCopy(it.Key()))
		lockVals = append(lockVals, safeCopy(it.Value()))
	}
	if len(lockKeys) == 0 {
		return nil
	}
	hashVals := keysToHashVals(lockKeys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	for i, lockKey := range lockKeys {
		buf = store.lockStore.Get(lockKey, buf)
		// We need to check again make sure the lock is not changed.
		if bytes.Equal(buf, lockVals[i]) {
			if commitTS > 0 {
				lock := mvcc.DecodeLock(lockVals[i])
				tmpDiff += len(lockKey) + len(lock.Value)
				batch.Commit(lockKey, &lock)
			} else {
				batch.Rollback(lockKey, true)
			}
		}
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) GC(reqCtx *requestCtx, safePoint uint64) error {
	// We use the gcLock to make sure safePoint can only increase.
	store.gcLock.Lock()
	defer store.gcLock.Unlock()
	oldSafePoint := atomic.LoadUint64(&store.safePoint.timestamp)
	if oldSafePoint < safePoint {
		err := store.db.Update(func(txn *badger.Txn) error {
			safePointValue := make([]byte, 8)
			binary.LittleEndian.PutUint64(safePointValue, safePoint)
			return txn.Set(InternalSafePointKey, safePointValue)
		})
		if err != nil {
			return err
		}
		atomic.StoreUint64(&store.safePoint.timestamp, safePoint)
	}
	return nil
}

type SafePoint struct {
	timestamp uint64
}

// CreateCompactionFilter implements badger.CompactionFilterFactory function.
func (sp *SafePoint) CreateCompactionFilter() badger.CompactionFilter {
	return &GCCompactionFilter{
		safePoint: atomic.LoadUint64(&sp.timestamp),
	}
}

// GCCompactionFilter implements the badger.CompactionFilter interface.
type GCCompactionFilter struct {
	safePoint uint64
}

// (old key first byte) = (latest key first byte) + 1
const (
	metaPrefix byte = 'm'
	// 'm' + 1 = 'n'
	metaOldPrefix byte = 'n'
	tablePrefix   byte = 't'
	// 't' + 1 = 'u
	tableOldPrefix byte = 'u'
)

// Filter implements the badger.CompactionFilter interface.
//
// The keys is divided into two sections, latest key and old key, the latest keys don't have commitTS appended,
// the old keys have commitTS appended and has a different key prefix.
//
// For old keys, if the nextCommitTS is before the safePoint, it means we already have one version before the safePoint,
// we can safely drop this entry.
//
// For latest keys, only Delete entry should be GCed, if the commitTS of the Delete entry is older than safePoint,
// we can remove the Delete entry. But we need to convert it to a tombstone instead of drop.
// Because there maybe multiple badger level old entries under the same key, dropping it results in old
// entries may appear again.
func (f *GCCompactionFilter) Filter(key, value, userMeta []byte) badger.Decision {
	switch key[0] {
	case metaPrefix, tablePrefix:
		// For latest version, we can only remove `delete` key, which has value len 0.
		if mvcc.DBUserMeta(userMeta).CommitTS() < f.safePoint && len(value) == 0 {
			return badger.DecisionMarkTombstone
		}
	case metaOldPrefix, tableOldPrefix:
		// The key is old version key.
		if mvcc.OldUserMeta(userMeta).NextCommitTS() < f.safePoint {
			return badger.DecisionDrop
		}
	}
	return badger.DecisionKeep
}
