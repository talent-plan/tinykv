package tikv

import (
	"bytes"
	"math"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/juju/errors"
)

func (store *MVCCStore) NewDBReader(reqCtx *requestCtx) *DBReader {
	return &DBReader{
		reqCtx: reqCtx,
		txn:    store.db.NewTransaction(false),
	}
}

func newIterator(txn *badger.Txn, reverse bool, startKey, endKey []byte) *badger.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = reverse
	opts.StartKey = y.KeyWithTs(startKey, math.MaxUint64)
	opts.EndKey = y.KeyWithTs(endKey, math.MaxUint64)
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
		regCtx := r.reqCtx.regCtx
		r.iter = newIterator(r.txn, false, regCtx.startKey, regCtx.endKey)
	}
	return r.iter
}

func (r *DBReader) getReverseIter() *badger.Iterator {
	if r.revIter == nil {
		regCtx := r.reqCtx.regCtx
		r.revIter = newIterator(r.txn, true, regCtx.startKey, regCtx.endKey)
	}
	return r.revIter
}

func (r *DBReader) getOldIter() *badger.Iterator {
	if r.oldIter == nil {
		regCtx := r.reqCtx.regCtx
		oldStartKey := safeCopy(regCtx.startKey)
		oldStartKey[0]++
		oldEndKey := safeCopy(regCtx.endKey)
		oldEndKey[0]++
		r.oldIter = newIterator(r.txn, false, oldStartKey, oldEndKey)
	}
	return r.oldIter
}

type BatchGetFunc = func(key, value []byte, err error)

func (r *DBReader) BatchGet(keys [][]byte, startTS uint64, f BatchGetFunc) {
	for _, key := range keys {
		val, err := r.Get(key, startTS)
		f(key, val, err)
	}
	return
}

type ScanFunc = func(key, value []byte) error

func (r *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64, f ScanFunc) error {
	iter := r.getIter()
	var cnt int
	var mvVal mvccValue
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		err := decodeValueTo(item, &mvVal)
		if err != nil {
			return errors.Trace(err)
		}
		if mvVal.commitTS > startTS {
			err = r.getOldValue(encodeOldKey(key, startTS), &mvVal)
			if err == badger.ErrKeyNotFound {
				continue
			}
		}
		if len(mvVal.value) == 0 {
			continue
		}
		err = f(key, mvVal.value)
		if err != nil {
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

func (r *DBReader) getOldValue(oldKey []byte, mvVal *mvccValue) error {
	oldIter := r.getOldIter()
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return badger.ErrKeyNotFound
	}
	return decodeValueTo(oldIter.Item(), mvVal)
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (r *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, f ScanFunc) error {
	iter := r.getReverseIter()
	var cnt int
	var mvVal mvccValue
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if bytes.Compare(key, startKey) < 0 {
			break
		}
		err := decodeValueTo(item, &mvVal)
		if err != nil {
			return errors.Trace(err)
		}
		if mvVal.commitTS > startTS {
			err = r.getOldValue(encodeOldKey(key, startTS), &mvVal)
			if err == badger.ErrKeyNotFound {
				continue
			}
		}
		if len(mvVal.value) == 0 {
			continue
		}
		f(key, mvVal.value)
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
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
