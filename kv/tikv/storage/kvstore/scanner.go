package kvstore

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and can not be used, or it is ready to return a value immediately.
type Scanner struct {
	writeIter engine_util.DBIterator
	txn       *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	writeIter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{
		writeIter: writeIter,
		txn:       txn,
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Search for the next relevant key/value.
	for {
		if !scan.writeIter.Valid() {
			// The underlying iterator is exhausted - we've reached the end of the DB.
			return nil, nil, nil
		}

		item := scan.writeIter.Item()
		userKey := decodeUserKey(item.Key())
		commitTs := decodeTimestamp(item.Key())

		if commitTs >= *scan.txn.StartTS {
			// The key was not committed before our transaction started, find an earlier key.
			scan.writeIter.Seek(EncodeKey(userKey, commitTs-1))
			continue
		}

		// Note: we might check if userKey is locked (since we should not read an uncommitted transaction). However,
		// because we are iterating over writes, we are guaranteed never to get a locked key at our timestamp (i.e., if
		// the key were locked, then we would use the older value, which is what we will get via the write in any case).

		writeValue, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(writeValue)
		if err != nil {
			return nil, nil, err
		}
		if write.Kind != WriteKindPut {
			// Key is removed, go to next key.
			scan.writeIter.Seek(EncodeKey(userKey, 0))
			continue
		}

		value, err := scan.txn.GetValue(userKey, write.StartTS)
		if err != nil {
			return nil, nil, err
		}

		scan.writeIter.Next()

		return userKey, value, nil
	}
}
