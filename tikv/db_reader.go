package tikv

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/juju/errors"
)

func (store *MVCCStore) NewDBReader(reqCtx *requestCtx) *DBReader {
	g := &DBReader{reqCtx: reqCtx}
	g.txn = store.db.NewTransaction(false)
	return g
}

func newIterator(txn *badger.Txn, reverse bool) *badger.Iterator {
	var itOpts = badger.DefaultIteratorOptions
	itOpts.PrefetchValues = false
	itOpts.Reverse = reverse
	return txn.NewIterator(itOpts)
}

// DBReader reads data from DB, for read-only requests, the locks must already be checked before DBReader is created.
type DBReader struct {
	reqCtx  *requestCtx
	txn     *badger.Txn
	iter    *badger.Iterator
	revIter *badger.Iterator
	oldIter *badger.Iterator
}

func (g *DBReader) Get(key []byte, startTS uint64) ([]byte, error) {
	item, err := g.txn.Get(key)
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
	iter := g.getIter()
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

func (g *DBReader) getIter() *badger.Iterator {
	if g.iter == nil {
		g.iter = newIterator(g.txn, false)
	}
	return g.iter
}

func (g *DBReader) getReverseIter() *badger.Iterator {
	if g.revIter == nil {
		g.revIter = newIterator(g.txn, true)
	}
	return g.revIter
}

func (g *DBReader) getOldIter() *badger.Iterator {
	if g.oldIter == nil {
		g.oldIter = newIterator(g.txn, false)
	}
	return g.oldIter
}

func (reader *DBReader) BatchGet(keys [][]byte, startTS uint64) []Pair {
	pairs := make([]Pair, 0, len(keys))
	for _, key := range keys {
		val, err := reader.Get(key, startTS)
		if len(val) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: val, Err: err})
	}
	return pairs
}

func (reader *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	iter := reader.getIter()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if exceedEndKey(item.Key(), endKey) {
			break
		}
		key := item.KeyCopy(nil)
		mvVal, err := decodeValue(item)
		if err != nil {
			return []Pair{{Err: err}}
		}
		if mvVal.commitTS > startTS {
			mvVal, err = reader.getOldValue(encodeOldKey(item.Key(), startTS))
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

func (reader *DBReader) getOldValue(oldKey []byte) (mvccValue, error) {
	oldIter := reader.getOldIter()
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return mvccValue{}, badger.ErrKeyNotFound
	}
	return decodeValue(oldIter.Item())
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (reader *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64) []Pair {
	var pairs []Pair
	iter := reader.getReverseIter()
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), startKey) < 0 {
			break
		}
		key := item.KeyCopy(nil)
		mvVal, err := decodeValue(item)
		if err != nil {
			return []Pair{{Err: err}}
		}
		if mvVal.commitTS > startTS {
			mvVal, err = reader.getOldValue(encodeOldKey(item.Key(), startTS))
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

func (g *DBReader) Close() {
	if g.iter != nil {
		g.iter.Close()
	}
	if g.oldIter != nil {
		g.oldIter.Close()
	}
	if g.revIter != nil {
		g.revIter.Close()
	}
	g.txn.Discard()
}
