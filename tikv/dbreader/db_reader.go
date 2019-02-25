package dbreader

import (
	"bytes"
	"math"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/juju/errors"
	"github.com/ngaut/unistore/tikv/mvcc"
)

func NewDBReader(startKey, endKey []byte, txn *badger.Txn, safePoint uint64) *DBReader {
	return &DBReader{
		startKey:  startKey,
		endKey:    endKey,
		txn:       txn,
		safePoint: safePoint,
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
	startKey  []byte
	endKey    []byte
	txn       *badger.Txn
	iter      *badger.Iterator
	revIter   *badger.Iterator
	oldIter   *badger.Iterator
	safePoint uint64
}

func (r *DBReader) Get(key []byte, startTS uint64) ([]byte, error) {
	item, err := r.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, errors.Trace(err)
	}
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if mvcc.DBUserMeta(item.UserMeta()).CommitTS() <= startTS {
		return item.Value()
	}
	oldKey := mvcc.EncodeOldKey(key, startTS)
	iter := r.GetIter()
	iter.Seek(oldKey)
	if !iter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return nil, nil
	}
	if mvcc.OldUserMeta(item.UserMeta()).NextCommitTS() < r.safePoint {
		// This entry is eligible for GC. Normally we will not see this version.
		// But when the latest version is DELETE and it is GCed first,
		// we may end up here, so we should ignore the obsolete version.
		return nil, nil
	}
	return iter.Item().Value()
}

func (r *DBReader) GetIter() *badger.Iterator {
	if r.iter == nil {
		r.iter = newIterator(r.txn, false, r.startKey, r.endKey)
	}
	return r.iter
}

func (r *DBReader) getReverseIter() *badger.Iterator {
	if r.revIter == nil {
		r.revIter = newIterator(r.txn, true, r.startKey, r.endKey)
	}
	return r.revIter
}

func (r *DBReader) GetOldIter() *badger.Iterator {
	if r.oldIter == nil {
		oldStartKey := append([]byte{}, r.startKey...)
		oldStartKey[0]++
		oldEndKey := append([]byte{}, r.endKey...)
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

// ScanBreak is returnd by ScanFunc to break the scan loop.
var ScanBreak = errors.New("scan break")

// ScanFunc accepts key and value, should not keep reference to them.
// Returns ScanBreak will break the scan loop.
type ScanFunc = func(key, value []byte) error

func (r *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64, f ScanFunc) error {
	if len(endKey) == 0 {
		panic("invalid end key")
	}

	iter := r.GetIter()
	var cnt int
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if bytes.Compare(key, endKey) >= 0 {
			break
		}
		var val []byte
		var err error
		if mvcc.DBUserMeta(item.UserMeta()).CommitTS() > startTS {
			val, err = r.getOldValue(mvcc.EncodeOldKey(key, startTS))
			if err == badger.ErrKeyNotFound {
				continue
			}
		} else {
			val, err = item.Value()
		}
		if err != nil {
			return errors.Trace(err)
		}
		if len(val) == 0 {
			continue
		}
		err = f(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

func (r *DBReader) getOldValue(oldKey []byte) ([]byte, error) {
	oldIter := r.GetOldIter()
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return nil, badger.ErrKeyNotFound
	}
	if mvcc.OldUserMeta(oldIter.Item().UserMeta()).NextCommitTS() < r.safePoint {
		// Ignore the obsolete version.
		return nil, badger.ErrKeyNotFound
	}
	return oldIter.Item().Value()
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (r *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, f ScanFunc) error {
	iter := r.getReverseIter()
	var cnt int
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if bytes.Compare(key, startKey) < 0 {
			break
		}
		var val []byte
		var err error
		if mvcc.DBUserMeta(item.UserMeta()).CommitTS() > startTS {
			val, err = r.getOldValue(mvcc.EncodeOldKey(key, startTS))
			if err == badger.ErrKeyNotFound {
				continue
			}
		} else {
			val, err = item.Value()
		}
		if err != nil {
			return errors.Trace(err)
		}
		if len(val) == 0 {
			continue
		}
		err = f(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

func (r *DBReader) GetTxn() *badger.Txn {
	return r.txn
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
