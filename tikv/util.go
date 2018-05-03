package tikv

import (
	"bytes"
	"github.com/dgraph-io/badger"
)

func updateWithRetry(db *badger.DB, updateFunc func(txn *badger.Txn) error) error {
	for i := 0; i < 10; i++ {
		err := db.Update(updateFunc)
		if err == nil {
			return nil
		}
		if err == badger.ErrConflict {
			continue
		}
		return err
	}
	return ErrRetryable("badger retry limit reached, try again later")
}

func exceedEndKey(current, endKey []byte) bool {
	return len(endKey) > 0 && bytes.Compare(current, endKey) >= 0
}
