package tikv

import (
	"sync"

	"github.com/coocood/badger"
	"github.com/juju/errors"
)

type writeBatch struct {
	entries []*badger.Entry
	err     error
	wg      sync.WaitGroup
}

func (batch *writeBatch) setWithMeta(key, val []byte, meta byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: meta,
	})
}

func (batch *writeBatch) delete(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		UserMeta: mixedDelFlag,
	})
}

func (store *MVCCStore) write(batch *writeBatch) error {
	if len(batch.entries) == 0 {
		return nil
	}
	batch.wg.Add(1)
	w := store.writeWorker
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

type writeWorker struct {
	mu struct {
		sync.Mutex
		batches []*writeBatch
	}
	wakeUp chan struct{}
	db     *badger.DB
}

func (w *writeWorker) run() {
	var batches []*writeBatch
	for {
		select {
		case <-w.wakeUp:
		}
		batches = batches[:0]
		w.mu.Lock()
		batches, w.mu.batches = w.mu.batches, batches
		w.mu.Unlock()
		err := w.db.Update(func(txn *badger.Txn) error {
			for _, batch := range batches {
				for _, entry := range batch.entries {
					var err error
					if entry.UserMeta == mixedDelFlag {
						err = txn.Delete(entry.Key)
					} else {
						err = txn.SetEntry(entry)
					}
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
			return nil
		})
		for _, batch := range batches {
			batch.err = err
			batch.wg.Done()
		}
	}
}
