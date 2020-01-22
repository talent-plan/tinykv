package dbreader

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type DBReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) *engine_util.CFIterator
}

type RegionReader struct {
	txn    *badger.Txn
	region *metapb.Region
}

func NewRegionReader(txn *badger.Txn, region metapb.Region) *RegionReader {
	return &RegionReader{
		txn:    txn,
		region: &region,
	}
}

func (r *RegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *RegionReader) IterCF(cf string) *engine_util.CFIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *RegionReader) Close() {
	r.txn.Discard()
}

// func NewDBReader(startKey, endKey []byte, txn *badger.Txn, safePoint uint64) *DBReader {
// 	return &DBReader{
// 		startKey:  startKey,
// 		endKey:    endKey,
// 		txn:       txn,
// 		safePoint: safePoint,
// 	}
// }

// func NewIterator(txn *badger.Txn, reverse bool, startKey, endKey []byte) *badger.Iterator {
// 	opts := badger.DefaultIteratorOptions
// 	opts.Reverse = reverse
// 	if len(startKey) > 0 {
// 		opts.StartKey = y.KeyWithTs(startKey, math.MaxUint64)
// 	}
// 	if len(endKey) > 0 {
// 		opts.EndKey = y.KeyWithTs(endKey, math.MaxUint64)
// 	}
// 	return txn.NewIterator(opts)
// }

// // DBReader reads data from DB, for read-only requests, the locks must already be checked before DBReader is created.
// type DBReader struct {
// 	startKey  []byte
// 	endKey    []byte
// 	txn       *badger.Txn
// 	iter      *badger.Iterator
// 	revIter   *badger.Iterator
// 	oldIter   *badger.Iterator
// 	safePoint uint64
// }

// // GetMvccInfoByKey fills MvccInfo reading committed keys from db
// func (r *DBReader) GetMvccInfoByKey(key []byte, isRowKey bool, mvccInfo *kvrpcpb.MvccInfo) error {
// 	err := r.getKeyWithMeta(key, isRowKey, uint64(math.MaxUint64), mvccInfo)
// 	if err != nil {
// 		return err
// 	}
// 	if len(mvccInfo.Writes) > 0 {
// 		oldKey := mvcc.EncodeOldKey(key, mvccInfo.Writes[0].CommitTs)
// 		err = r.getOldKeysWithMeta(oldKey, isRowKey, mvccInfo)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// // getKeyWithMeta will try to get current key without looking for historical version
// func (r *DBReader) getKeyWithMeta(key []byte, isRowKey bool, startTs uint64, mvccInfo *kvrpcpb.MvccInfo) error {
// 	item, err := r.txn.Get(key)
// 	if err != nil && err != badger.ErrKeyNotFound {
// 		return errors.Trace(err)
// 	}
// 	if err == badger.ErrKeyNotFound {
// 		return nil
// 	}
// 	dbUsrMeta := mvcc.DBUserMeta(item.UserMeta())
// 	if dbUsrMeta.CommitTS() <= startTs {
// 		var val []byte
// 		if isRowKey {
// 			val, err = item.Value()
// 			if err != nil {
// 				return err
// 			}
// 			val, err = rowcodec.RowToOldRow(val, nil)
// 			if err != nil {
// 				return err
// 			}
// 		} else {
// 			val, err = item.ValueCopy(nil)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		curRecord := &kvrpcpb.MvccWrite{
// 			Type:       kvrpcpb.Op_Put,
// 			StartTs:    dbUsrMeta.StartTS(),
// 			CommitTs:   dbUsrMeta.CommitTS(),
// 			ShortValue: val,
// 		}
// 		mvccInfo.Writes = append(mvccInfo.Writes, curRecord)
// 	}
// 	return nil
// }

// // getOldKeysWithMeta will try to fill mvccInfo with all the historical committed records
// // the oldKey should be in old-key encoded format
// func (r *DBReader) getOldKeysWithMeta(oldKey []byte, isRowKey bool, mvccInfo *kvrpcpb.MvccInfo) error {
// 	oldIter := r.GetOldIter()
// 	for oldIter.Seek(oldKey); oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]); oldIter.Next() {
// 		item := oldIter.Item()
// 		oldUsrMeta := mvcc.OldUserMeta(item.UserMeta())
// 		commitTs, err := mvcc.DecodeOldKeyCommitTs(item.Key())
// 		if err != nil {
// 			return err
// 		}
// 		var val []byte
// 		if isRowKey {
// 			val, err = item.Value()
// 			if err != nil {
// 				return err
// 			}
// 			val, err = rowcodec.RowToOldRow(val, nil)
// 			if err != nil {
// 				return err
// 			}
// 		} else {
// 			val, err = item.ValueCopy(nil)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		curRecord := &kvrpcpb.MvccWrite{
// 			Type:       kvrpcpb.Op_Put,
// 			StartTs:    oldUsrMeta.StartTS(),
// 			CommitTs:   commitTs,
// 			ShortValue: val,
// 		}
// 		mvccInfo.Writes = append(mvccInfo.Writes, curRecord)
// 	}

// 	return nil
// }

// func (r *DBReader) Get(key []byte, startTS uint64) ([]byte, error) {
// 	item, err := r.txn.Get(key)
// 	if err != nil && err != badger.ErrKeyNotFound {
// 		return nil, errors.Trace(err)
// 	}
// 	if err == badger.ErrKeyNotFound {
// 		return nil, nil
// 	}
// 	if mvcc.DBUserMeta(item.UserMeta()).CommitTS() <= startTS {
// 		return item.Value()
// 	}
// 	item = r.getOldItem(key, startTS)
// 	if item != nil && !item.IsEmpty() {
// 		return item.Value()
// 	}
// 	return nil, nil
// }

// func (r *DBReader) GetIter() *badger.Iterator {
// 	if r.iter == nil {
// 		r.iter = NewIterator(r.txn, false, r.startKey, r.endKey)
// 	}
// 	return r.iter
// }

// func (r *DBReader) getReverseIter() *badger.Iterator {
// 	if r.revIter == nil {
// 		r.revIter = NewIterator(r.txn, true, r.startKey, r.endKey)
// 	}
// 	return r.revIter
// }

// func (r *DBReader) GetOldIter() *badger.Iterator {
// 	if r.oldIter == nil {
// 		oldStartKey := append([]byte{}, r.startKey...)
// 		if len(oldStartKey) > 0 {
// 			oldStartKey[0]++
// 		}
// 		oldEndKey := append([]byte{}, r.endKey...)
// 		if len(oldEndKey) > 0 {
// 			oldEndKey[0]++
// 		}
// 		r.oldIter = NewIterator(r.txn, false, oldStartKey, oldEndKey)
// 	}
// 	return r.oldIter
// }

// type BatchGetFunc = func(key, value []byte, err error)

// func (r *DBReader) BatchGet(keys [][]byte, startTS uint64, f BatchGetFunc) {
// 	items, err := r.txn.MultiGet(keys)
// 	if err != nil {
// 		for _, key := range keys {
// 			f(key, nil, err)
// 		}
// 		return
// 	}
// 	for i, item := range items {
// 		key := keys[i]
// 		var val []byte
// 		if item != nil {
// 			if mvcc.DBUserMeta(item.UserMeta()).CommitTS() <= startTS {
// 				val, err = item.Value()
// 			} else {
// 				item = r.getOldItem(keys[i], startTS)
// 				if item != nil && !item.IsEmpty() {
// 					val, err = item.Value()
// 				}
// 			}
// 		}
// 		f(key, val, err)
// 	}
// 	return
// }

// // ScanBreak is returnd by ScanFunc to break the scan loop.
// var ScanBreak = errors.New("scan break")

// // ScanFunc accepts key and value, should not keep reference to them.
// // Returns ScanBreak will break the scan loop.
// type ScanFunc = func(key, value []byte) error

// // ScanProcessor process the key/value pair.
// type ScanProcessor interface {
// 	// Process accepts key and value, should not keep reference to them.
// 	// Returns ScanBreak will break the scan loop.
// 	Process(key, value []byte) error
// 	// SkipValue returns if we can skip the value.
// 	SkipValue() bool
// }

// func (r *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
// 	if len(endKey) == 0 {
// 		panic("invalid end key")
// 	}
// 	skipValue := proc.SkipValue()
// 	iter := r.GetIter()
// 	var cnt int
// 	for iter.Seek(startKey); iter.Valid(); iter.Next() {
// 		item := iter.Item()
// 		key := item.Key()
// 		if bytes.Compare(key, endKey) >= 0 {
// 			break
// 		}
// 		var err error
// 		if mvcc.DBUserMeta(item.UserMeta()).CommitTS() > startTS {
// 			item = r.getOldItem(key, startTS)
// 			if item == nil {
// 				continue
// 			}
// 		}
// 		if item.IsEmpty() {
// 			continue
// 		}
// 		var val []byte
// 		if !skipValue {
// 			val, err = item.Value()
// 			if err != nil {
// 				return errors.Trace(err)
// 			}
// 		}
// 		err = proc.Process(key, val)
// 		if err != nil {
// 			if err == ScanBreak {
// 				break
// 			}
// 			return errors.Trace(err)
// 		}
// 		cnt++
// 		if cnt >= limit {
// 			break
// 		}
// 	}
// 	return nil
// }

// func (r *DBReader) GetKeyByStartTs(startKey, endKey []byte, startTs uint64) ([]byte, error) {
// 	iter := r.GetIter()
// 	for iter.Seek(startKey); iter.Valid(); iter.Next() {
// 		curItem := iter.Item()
// 		curKey := curItem.Key()
// 		if bytes.Compare(curKey, endKey) >= 0 {
// 			break
// 		}
// 		meta := mvcc.DBUserMeta(curItem.UserMeta())
// 		if meta.StartTS() == startTs {
// 			return curItem.KeyCopy(nil), nil
// 		}
// 	}
// 	oldIter := r.GetOldIter()
// 	oldStartKey := append([]byte{}, startKey...)
// 	oldStartKey[0]++
// 	oldEndKey := append([]byte{}, endKey...)
// 	oldEndKey[0]++
// 	for oldIter.Seek(oldStartKey); oldIter.Valid(); oldIter.Next() {
// 		curItem := oldIter.Item()
// 		oldKey := curItem.Key()
// 		if bytes.Compare(oldKey, oldEndKey) >= 0 {
// 			break
// 		}
// 		oldMeta := mvcc.OldUserMeta(curItem.UserMeta())
// 		if oldMeta.StartTS() == startTs {
// 			rawKey, _, err := mvcc.DecodeOldKey(oldKey)
// 			if err != nil {
// 				return nil, err
// 			}
// 			return rawKey, nil
// 		}
// 	}
// 	return nil, nil
// }

// func (r *DBReader) getOldItem(key []byte, startTS uint64) *badger.Item {
// 	oldKey := mvcc.EncodeOldKey(key, startTS)
// 	oldIter := r.GetOldIter()
// 	oldIter.Seek(oldKey)
// 	for oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
// 		item := oldIter.Item()
// 		nextCommitTS := mvcc.OldUserMeta(item.UserMeta()).NextCommitTS()
// 		if nextCommitTS < r.safePoint {
// 			// Ignore the obsolete version.
// 			return nil
// 		}
// 		if nextCommitTS == mvcc.DecodeKeyTS(item.Key()) {
// 			// Ignore Op_Lock old entry.
// 			oldIter.Next()
// 			continue
// 		}
// 		return item
// 	}
// 	return nil
// }

// // ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
// func (r *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
// 	skipValue := proc.SkipValue()
// 	iter := r.getReverseIter()
// 	var cnt int
// 	for iter.Seek(endKey); iter.Valid(); iter.Next() {
// 		item := iter.Item()
// 		key := item.Key()
// 		if bytes.Compare(key, startKey) < 0 {
// 			break
// 		}
// 		if cnt == 0 && bytes.Equal(key, endKey) {
// 			continue
// 		}
// 		var err error
// 		if mvcc.DBUserMeta(item.UserMeta()).CommitTS() > startTS {
// 			item = r.getOldItem(key, startTS)
// 			if item == nil {
// 				continue
// 			}
// 		}
// 		if item.IsEmpty() {
// 			continue
// 		}
// 		var val []byte
// 		if !skipValue {
// 			val, err = item.Value()
// 			if err != nil {
// 				return errors.Trace(err)
// 			}
// 		}
// 		err = proc.Process(key, val)
// 		if err != nil {
// 			if err == ScanBreak {
// 				break
// 			}
// 			return errors.Trace(err)
// 		}
// 		cnt++
// 		if cnt >= limit {
// 			break
// 		}
// 	}
// 	return nil
// }

// func (r *DBReader) GetTxn() *badger.Txn {
// 	return r.txn
// }

// func (r *DBReader) Close() {
// 	if r.iter != nil {
// 		r.iter.Close()
// 	}
// 	if r.oldIter != nil {
// 		r.oldIter.Close()
// 	}
// 	if r.revIter != nil {
// 		r.revIter.Close()
// 	}
// 	r.txn.Discard()
// }
