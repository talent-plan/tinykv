package tikv

import (
	"bytes"
	"time"

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

func extractPhysicalTime(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.Unix(t/1e3, (t%1e3)*1e6)
}
