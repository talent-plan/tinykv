package tikv

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/juju/errors"
)

func (store *MVCCStore) NewDBReader(reqCtx *requestCtx) *DBReader {
	return &DBReader{
		reqCtx: reqCtx,
		txn:    store.db.NewTransaction(false),
	}
}

func newIterator(txn *badger.Txn, reverse bool) *badger.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = reverse
	return txn.NewIterator(opts)
}

// DBReader reads data from DB, for read-only requests, the locks must already be checked before DBReader is created.
type DBReader struct {
	reqCtx  *requestCtx
	txn     *badger.Txn
	iter    *badger.Iterator
	revIter *badger.Iterator
	oldIter *badger.Iterator
}

func (r *DBReader) Get(key []byte, startTS uint64) ([]byte, error) {
	item, err := r.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, errors.Trace(err)
	}
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	mvVal, err := decodeValue(item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if mvVal.commitTS <= startTS {
		return mvVal.value, nil
	}
	oldKey := encodeOldKey(key, startTS)
	iter := r.getIter()
	iter.Seek(oldKey)
	if !iter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return nil, nil
	}
	item = iter.Item()
	mvVal, err = decodeValue(item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mvVal.value, nil
}

func (r *DBReader) getIter() *badger.Iterator {
	if r.iter == nil {
		r.iter = newIterator(r.txn, false)
	}
	return r.iter
}

func (r *DBReader) getReverseIter() *badger.Iterator {
	if r.revIter == nil {
		r.revIter = newIterator(r.txn, true)
	}
	return r.revIter
}

func (r *DBReader) getOldIter() *badger.Iterator {
	if r.oldIter == nil {
		r.oldIter = newIterator(r.txn, false)
	}
	return r.oldIter
}

func (r *DBReader) BatchGet(keys [][]byte, startTS uint64) []Pair {
	pairs := make([]Pair, 0, len(keys))
	for _, key := range keys {
		val, err := r.Get(key, startTS)
		if len(val) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: val, Err: err})
	}
	return pairs
}

func (r *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	iter := r.getIter()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		mvVal, err := decodeValue(item)
		if err != nil {
			return []Pair{{Err: err}}
		}
		if mvVal.commitTS > startTS {
			mvVal, err = r.getOldValue(encodeOldKey(key, startTS))
			if err == badger.ErrKeyNotFound {
				continue
			}
		}
		if len(mvVal.value) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: mvVal.value})
		if len(pairs) >= limit {
			break
		}
	}
	return pairs
}

func (r *DBReader) getOldValue(oldKey []byte) (mvccValue, error) {
	oldIter := r.getOldIter()
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return mvccValue{}, badger.ErrKeyNotFound
	}
	return decodeValue(oldIter.Item())
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (r *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	iter := r.getReverseIter()
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		if bytes.Compare(key, startKey) < 0 {
			break
		}
		mvVal, err := decodeValue(item)
		if err != nil {
			return []Pair{{Err: err}}
		}
		if mvVal.commitTS > startTS {
			mvVal, err = r.getOldValue(encodeOldKey(key, startTS))
			if err == badger.ErrKeyNotFound {
				continue
			}
		}
		if len(mvVal.value) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: mvVal.value})
		if len(pairs) >= limit {
			break
		}
	}
	return pairs
}

func (r *DBReader) Close() {
	if r.iter != nil {
		r.iter.Close()
	}
	if r.oldIter != nil {
		r.oldIter.Close()
	}
	if r.revIter != nil {
		r.revIter.Close()
	}
	r.txn.Discard()
}
