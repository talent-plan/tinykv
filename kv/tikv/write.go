package tikv

// import (
// 	"bytes"
// 	"math"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"github.com/cznic/mathutil"
// 	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
// 	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

// 	"github.com/coocood/badger"
// 	"github.com/pingcap-incubator/tinykv/kv/tikv/mvcc"
// )

// const (
// 	batchChanSize = 1024
// )

// type writeDBBatch struct {
// 	entries []*badger.Entry
// 	err     error
// 	wg      sync.WaitGroup
// }

// func newWriteDBBatch() *writeDBBatch {
// 	return &writeDBBatch{}
// }

// func (batch *writeDBBatch) set(key, val []byte, userMeta []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key:      key,
// 		Value:    val,
// 		UserMeta: userMeta,
// 	})
// }

// // delete is a badger level operation, only used in DeleteRange, so we don't need to set UserMeta.
// // Then we can tell the entry is delete if UserMeta is nil.
// func (batch *writeDBBatch) delete(key []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key: key,
// 	})
// }

// type writeLockBatch struct {
// 	entries []*badger.Entry
// 	err     error
// 	wg      sync.WaitGroup
// }

// func (batch *writeLockBatch) set(key, val []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key:      key,
// 		Value:    val,
// 		UserMeta: mvcc.LockUserMetaNone,
// 	})
// }

// func (batch *writeLockBatch) rollback(key []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key:      key,
// 		UserMeta: mvcc.LockUserMetaRollback,
// 	})
// }

// func (batch *writeLockBatch) rollbackGC(key []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key:      key,
// 		UserMeta: mvcc.LockUserMetaRollbackGC,
// 	})
// }

// func (batch *writeLockBatch) delete(key []byte) {
// 	batch.entries = append(batch.entries, &badger.Entry{
// 		Key:      key,
// 		UserMeta: mvcc.LockUserMetaDelete,
// 	})
// }

// type writeDBWorker struct {
// 	batchCh chan *writeDBBatch
// 	writer  *dbWriter
// }

// func (w writeDBWorker) run() {
// 	defer w.writer.wg.Done()
// 	var batches []*writeDBBatch
// 	for {
// 		batches = batches[:0]
// 		select {
// 		case <-w.writer.closeCh:
// 			return
// 		case batch := <-w.batchCh:
// 			batches = append(batches, batch)
// 		}
// 		chLen := len(w.batchCh)
// 		for i := 0; i < chLen; i++ {
// 			batches = append(batches, <-w.batchCh)
// 		}
// 		if len(batches) > 0 {
// 			w.updateBatchGroup(batches)
// 		}
// 	}
// }

// func (w writeDBWorker) updateBatchGroup(batchGroup []*writeDBBatch) {
// 	e := w.writer.bundle.DB.Update(func(txn *badger.Txn) error {
// 		for _, batch := range batchGroup {
// 			for _, entry := range batch.entries {
// 				var err error
// 				if len(entry.UserMeta) == 0 {
// 					err = txn.Delete(entry.Key)
// 				} else {
// 					err = txn.SetEntry(entry)
// 				}
// 				if err != nil {
// 					return err
// 				}
// 			}
// 		}
// 		return nil
// 	})
// 	for _, batch := range batchGroup {
// 		batch.err = e
// 		batch.wg.Done()
// 	}
// }

// type writeLockWorker struct {
// 	batchCh chan *writeLockBatch
// 	writer  *dbWriter
// }

// func (w writeLockWorker) run() {
// 	defer w.writer.wg.Done()
// 	rollbackStore := w.writer.bundle.RollbackStore
// 	ls := w.writer.bundle.LockStore
// 	var batches []*writeLockBatch
// 	for {
// 		batches = batches[:0]
// 		select {
// 		case <-w.writer.closeCh:
// 			return
// 		case batch := <-w.batchCh:
// 			batches = append(batches, batch)
// 		}
// 		chLen := len(w.batchCh)
// 		for i := 0; i < chLen; i++ {
// 			batches = append(batches, <-w.batchCh)
// 		}
// 		var delCnt, insertCnt int
// 		for _, batch := range batches {
// 			for _, entry := range batch.entries {
// 				switch entry.UserMeta[0] {
// 				case mvcc.LockUserMetaRollbackByte:
// 					rollbackStore.Insert(entry.Key, []byte{0})
// 				case mvcc.LockUserMetaDeleteByte:
// 					delCnt++
// 					if !ls.Delete(entry.Key) {
// 						panic("failed to delete key")
// 					}
// 				case mvcc.LockUserMetaRollbackGCByte:
// 					rollbackStore.Delete(entry.Key)
// 				default:
// 					insertCnt++
// 					if !ls.Insert(entry.Key, entry.Value) {
// 						panic("failed to insert key")
// 					}
// 				}
// 			}
// 			batch.wg.Done()
// 		}
// 	}
// }

// // rollbackGCWorker delete all rollback keys after one minute to recycle memory.
// type rollbackGCWorker struct {
// 	writer *dbWriter
// }

// func (w rollbackGCWorker) run() {
// 	defer w.writer.wg.Done()
// 	ticker := time.Tick(time.Minute)
// 	rollbackStore := w.writer.bundle.RollbackStore
// 	for {
// 		select {
// 		case <-w.writer.closeCh:
// 			return
// 		case <-ticker:
// 		}
// 		lockBatch := &writeLockBatch{}
// 		it := rollbackStore.NewIterator()
// 		latestTS := w.writer.getLatestTS()
// 		for it.SeekToFirst(); it.Valid(); it.Next() {
// 			ts := mvcc.DecodeKeyTS(it.Key())
// 			if tsSub(latestTS, ts) > time.Minute {
// 				lockBatch.rollbackGC(safeCopy(it.Key()))
// 			}
// 			if len(lockBatch.entries) >= 1000 {
// 				lockBatch.wg.Add(1)
// 				w.writer.lockCh <- lockBatch
// 				lockBatch.wg.Wait()
// 				lockBatch.entries = lockBatch.entries[:0]
// 			}
// 		}
// 		if len(lockBatch.entries) == 0 {
// 			continue
// 		}
// 		lockBatch.wg.Add(1)
// 		w.writer.lockCh <- lockBatch
// 		lockBatch.wg.Wait()
// 	}
// }

// type dbWriter struct {
// 	bundle    *mvcc.DBBundle
// 	safePoint *SafePoint
// 	dbCh      chan<- *writeDBBatch
// 	lockCh    chan<- *writeLockBatch
// 	wg        sync.WaitGroup
// 	closeCh   chan struct{}
// 	latestTS  uint64
// }

// func NewDBWriter(bundle *mvcc.DBBundle, safePoint *SafePoint) mvcc.DBWriter {
// 	return &dbWriter{
// 		bundle:    bundle,
// 		safePoint: safePoint,
// 		closeCh:   make(chan struct{}, 0),
// 	}
// }

// func (writer *dbWriter) Open() {
// 	writer.wg.Add(3)

// 	dbCh := make(chan *writeDBBatch, batchChanSize)
// 	writer.dbCh = dbCh
// 	go writeDBWorker{
// 		batchCh: dbCh,
// 		writer:  writer,
// 	}.run()

// 	lockCh := make(chan *writeLockBatch, batchChanSize)
// 	writer.lockCh = lockCh
// 	go writeLockWorker{
// 		batchCh: lockCh,
// 		writer:  writer,
// 	}.run()

// 	go rollbackGCWorker{
// 		writer: writer,
// 	}.run()
// }

// func (writer *dbWriter) Close() {
// 	close(writer.closeCh)
// 	writer.wg.Wait()
// }

// func (writer *dbWriter) Write(batch mvcc.WriteBatch) error {
// 	wb := batch.(*writeBatch)
// 	if len(wb.dbBatch.entries) > 0 {
// 		wb.dbBatch.wg.Add(1)
// 		writer.dbCh <- &wb.dbBatch
// 		wb.dbBatch.wg.Wait()
// 		err := wb.dbBatch.err
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	if len(wb.lockBatch.entries) > 0 {
// 		// We must delete lock after commit succeed, or there will be inconsistency.
// 		wb.lockBatch.wg.Add(1)
// 		writer.lockCh <- &wb.lockBatch
// 		wb.lockBatch.wg.Wait()
// 		return wb.lockBatch.err
// 	}
// 	return nil
// }

// type writeBatch struct {
// 	startTS   uint64
// 	commitTS  uint64
// 	dbBatch   writeDBBatch
// 	lockBatch writeLockBatch
// }

// func (wb *writeBatch) Prewrite(key []byte, lock *mvcc.MvccLock, isPessimisticLock bool) {
// 	if isPessimisticLock {
// 		wb.lockBatch.delete(key)
// 	}
// 	wb.lockBatch.set(key, lock.MarshalBinary())
// }

// func (wb *writeBatch) Commit(key []byte, lock *mvcc.MvccLock) {
// 	userMeta := mvcc.NewDBUserMeta(wb.startTS, wb.commitTS)
// 	if lock.Op != uint8(kvrpcpb.Op_Lock) {
// 		wb.dbBatch.set(key, lock.Value, userMeta)
// 		if lock.HasOldVer {
// 			oldKey := mvcc.EncodeOldKey(key, lock.OldMeta.CommitTS())
// 			wb.dbBatch.set(oldKey, lock.OldVal, lock.OldMeta.ToOldUserMeta(wb.commitTS))
// 		}
// 	} else if bytes.Equal(key, lock.Primary) {
// 		// For primary key with Op_Lock type, the value need to be skipped, but we need to keep the transaction status.
// 		// So we put it as old key directly.
// 		if lock.HasOldVer {
// 			// There is a latest value, we don't want to move the value to the old key space for performance and simplicity.
// 			// Write the lock as old key directly to store the transaction status.
// 			// The old entry doesn't have value as Delete entry, but we can compare the commitTS in key and NextCommitTS
// 			// in the user meta to determine if the entry is Delete or Op_Lock.
// 			// If NextCommitTS equals CommitTS, it is Op_Lock, otherwise, it is Delete.
// 			oldKey := mvcc.EncodeOldKey(key, wb.commitTS)
// 			wb.dbBatch.set(oldKey, nil, userMeta)
// 		} else {
// 			// Convert the lock to a delete to store the transaction status.
// 			wb.dbBatch.set(key, nil, userMeta)
// 		}
// 	}
// 	wb.lockBatch.delete(key)
// }

// func (wb *writeBatch) Rollback(key []byte, deleteLock bool) {
// 	rollbackKey := mvcc.EncodeRollbackKey(nil, key, wb.startTS)
// 	wb.lockBatch.rollback(rollbackKey)
// 	if deleteLock {
// 		wb.lockBatch.delete(key)
// 	}
// }

// func (wb *writeBatch) PessimisticLock(key []byte, lock *mvcc.MvccLock) {
// 	wb.lockBatch.set(key, lock.MarshalBinary())
// }

// func (wb *writeBatch) PessimisticRollback(key []byte) {
// 	wb.lockBatch.delete(key)
// }

// func (writer *dbWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
// 	if commitTS > 0 {
// 		writer.updateLatestTS(commitTS)
// 	} else {
// 		writer.updateLatestTS(startTS)
// 	}
// 	return &writeBatch{
// 		startTS:  startTS,
// 		commitTS: commitTS,
// 	}
// }

// func (writer *dbWriter) getLatestTS() uint64 {
// 	return atomic.LoadUint64(&writer.latestTS)
// }

// func (writer *dbWriter) updateLatestTS(ts uint64) {
// 	latestTS := writer.getLatestTS()
// 	if ts != math.MaxUint64 && ts > latestTS {
// 		atomic.CompareAndSwapUint64(&writer.latestTS, latestTS, ts)
// 	}
// }

// const delRangeBatchSize = 4096

// func (writer *dbWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
// 	keys := make([][]byte, 0, delRangeBatchSize)
// 	oldStartKey := mvcc.EncodeOldKey(startKey, maxSystemTS)
// 	oldEndKey := mvcc.EncodeOldKey(endKey, maxSystemTS)
// 	txn := writer.bundle.DB.NewTransaction(false)
// 	reader := dbreader.NewDBReader(startKey, endKey, txn, atomic.LoadUint64(&writer.safePoint.timestamp))
// 	keys = writer.collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
// 	keys = writer.collectRangeKeys(reader.GetIter(), oldStartKey, oldEndKey, keys)
// 	reader.Close()
// 	return writer.deleteKeysInBatch(latchHandle, keys, delRangeBatchSize)
// }

// func (writer *dbWriter) collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
// 	if len(endKey) == 0 {
// 		panic("invalid end key")
// 	}
// 	for it.Seek(startKey); it.Valid(); it.Next() {
// 		item := it.Item()
// 		key := item.KeyCopy(nil)
// 		if exceedEndKey(key, endKey) {
// 			break
// 		}
// 		keys = append(keys, key)
// 	}
// 	return keys
// }

// func (writer *dbWriter) deleteKeysInBatch(latchHandle mvcc.LatchHandle, keys [][]byte, batchSize int) error {
// 	for len(keys) > 0 {
// 		batchSize := mathutil.Min(len(keys), batchSize)
// 		batchKeys := keys[:batchSize]
// 		keys = keys[batchSize:]
// 		hashVals := keysToHashVals(batchKeys...)
// 		dbBatch := newWriteDBBatch()
// 		for _, key := range batchKeys {
// 			dbBatch.delete(key)
// 		}
// 		latchHandle.AcquireLatches(hashVals)
// 		dbBatch.wg.Add(1)
// 		writer.dbCh <- dbBatch
// 		dbBatch.wg.Wait()
// 		latchHandle.ReleaseLatches(hashVals)
// 		if dbBatch.err != nil {
// 			return dbBatch.err
// 		}
// 	}
// 	return nil
// }
