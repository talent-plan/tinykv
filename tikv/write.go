package tikv

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/juju/errors"
)

type writeDBBatch struct {
	entries []*badger.Entry
	buf     []byte
	err     error
	wg      sync.WaitGroup
	reqCtx  *requestCtx
}

func newWriteDBBatch(reqCtx *requestCtx) *writeDBBatch {
	return &writeDBBatch{reqCtx: reqCtx}
}

func (batch *writeDBBatch) set(key, val []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
}

func (batch *writeDBBatch) delete(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		UserMeta: userMetaDelete,
	})
}

func (batch *writeDBBatch) size() int64 {
	var s int
	for _, entry := range batch.entries {
		s += len(entry.Key) + len(entry.Value)
	}
	return int64(s)
}

type writeLockBatch struct {
	entries []*badger.Entry
	buf     []byte
	err     error
	wg      sync.WaitGroup
	reqCtx  *requestCtx
}

func newWriteLockBatch(reqCtx *requestCtx) *writeLockBatch {
	return &writeLockBatch{reqCtx: reqCtx}
}

func (batch *writeLockBatch) set(key, val []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
}

func (batch *writeLockBatch) rollback(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		UserMeta: userMetaRollback,
	})
}

func (batch *writeLockBatch) rollbackGC(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		UserMeta: userMetaRollbackGC,
	})
}

func (batch *writeLockBatch) delete(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		UserMeta: userMetaDelete,
	})
}

func (store *MVCCStore) writeDB(batch *writeDBBatch) error {
	if len(batch.entries) == 0 {
		return nil
	}
	batch.wg.Add(1)
	w := store.writeDBWorker
	w.mu.Lock()
	w.mu.batches = append(w.mu.batches, batch)
	w.mu.Unlock()
	select {
	case w.wakeUp <- struct{}{}:
	default:
	}
	batch.wg.Wait()
	return batch.err
}

func (store *MVCCStore) writeLocks(batch *writeLockBatch) error {
	if len(batch.entries) == 0 {
		return nil
	}
	batch.wg.Add(1)
	w := store.writeLockWorker
	w.mu.Lock()
	w.mu.batches = append(w.mu.batches, batch)
	w.mu.Unlock()
	select {
	case w.wakeUp <- struct{}{}:
	default:
	}
	batch.wg.Wait()
	return batch.err
}

type writeDBWorker struct {
	mu struct {
		sync.Mutex
		batches []*writeDBBatch
	}
	wakeUp  chan struct{}
	closeCh <-chan struct{}
	store   *MVCCStore
}

func (w *writeDBWorker) run() {
	defer w.store.wg.Done()
	var batches []*writeDBBatch
	for {
		select {
		case <-w.store.closeCh:
			return
		case <-w.wakeUp:
		}
		batches = batches[:0]
		w.mu.Lock()
		batches, w.mu.batches = w.mu.batches, batches
		w.mu.Unlock()
		if len(batches) > 0 {
			batchesGroups := w.splitBatches(batches)
			for _, batchGroup := range batchesGroups {
				w.updateBatchGroup(batchGroup)
			}
		}
	}
}

func (w *writeDBWorker) splitBatches(batches []*writeDBBatch) [][]*writeDBBatch {
	splitOffsets := []int{0}
	var batchGroupEntries int
	for i, batch := range batches {
		batchGroupEntries += len(batch.entries)
		if batchGroupEntries > 4<<10 || i == len(batches)-1 {
			batchGroupEntries = 0
			splitOffsets = append(splitOffsets, i+1)
		}
	}

	batchGroups := make([][]*writeDBBatch, len(splitOffsets))
	for i := 1; i < len(splitOffsets); i++ {
		batchGroups[i] = batches[splitOffsets[i-1]:splitOffsets[i]]
	}
	return batchGroups
}

func (w *writeDBWorker) updateBatchGroup(batchGroup []*writeDBBatch) {
	begin := time.Now()
	var in time.Time
	err := w.store.db.Update(func(txn *badger.Txn) error {
		for _, batch := range batchGroup {
			for _, entry := range batch.entries {
				err := txn.SetEntry(entry)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		in = time.Now()
		return nil
	})
	end := time.Now()
	for _, batch := range batchGroup {
		batch.reqCtx.traceAt(eventBeginWriteDB, begin)
		batch.reqCtx.traceAt(eventInWriteDB, in)
		batch.reqCtx.traceAt(eventEndWriteDB, end)
		batch.err = err
		batch.wg.Done()
	}
}

type writeLockWorker struct {
	mu struct {
		sync.Mutex
		batches []*writeLockBatch
	}
	wakeUp  chan struct{}
	closeCh <-chan struct{}
	store   *MVCCStore
}

func (w *writeLockWorker) run() {
	defer w.store.wg.Done()
	rollbackStore := w.store.rollbackStore
	ls := w.store.lockStore
	var batches []*writeLockBatch
	for {
		select {
		case <-w.store.closeCh:
			return
		case <-w.wakeUp:
		}
		batches = batches[:0]
		w.mu.Lock()
		batches, w.mu.batches = w.mu.batches, batches
		w.mu.Unlock()
		begin := time.Now()
		for _, batch := range batches {
			batch.reqCtx.traceAt(eventBeginWriteLock, begin)
		}
		var delCnt, insertCnt int
		for _, batch := range batches {
			for _, entry := range batch.entries {
				switch entry.UserMeta {
				case userMetaRollback:
					w.store.rollbackStore.Insert(entry.Key, []byte{0})
				case userMetaDelete:
					delCnt++
					if !ls.Delete(entry.Key) {
						panic("failed to delete key")
					}
				case userMetaRollbackGC:
					rollbackStore.Delete(entry.Key)
				default:
					insertCnt++
					if !ls.Insert(entry.Key, entry.Value) {
						panic("failed to insert key")
					}
				}
			}
			batch.wg.Done()
		}
	}
}

// rollbackGCWorker delete all rollback keys after one minute to recycle memory.
type rollbackGCWorker struct {
	store *MVCCStore
}

func (w *rollbackGCWorker) run() {
	store := w.store
	defer store.wg.Done()
	ticker := time.Tick(time.Minute)
	for {
		select {
		case <-store.closeCh:
			return
		case <-ticker:
		}
		lockBatch := newWriteLockBatch(new(requestCtx))
		it := store.rollbackStore.NewIterator()
		latestTS := store.getLatestTS()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			ts := decodeRollbackTS(it.Key())
			if tsSub(latestTS, ts) > time.Minute {
				lockBatch.rollbackGC(safeCopy(it.Key()))
			}
			if len(lockBatch.entries) >= 1000 {
				store.writeLocks(lockBatch)
				lockBatch.entries = lockBatch.entries[:0]
			}
		}
		if len(lockBatch.entries) == 0 {
			continue
		}
		store.writeLocks(lockBatch)
	}
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
