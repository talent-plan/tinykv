package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type Reader struct {
	txn *badger.Txn
}

func NewReader(txn *badger.Txn) *Reader {
	return &Reader{
		txn: txn,
	}
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
	// 这部分代码就是对`NewCFIterator`外套了一层，没有拓展的功能可以省略
	// return NewReaderIterator(engine_util.NewCFIterator(cf, r.txn))
}

func (r *Reader) Close() {
	r.txn.Discard()
}

// 这部分代码就是对`NewCFIterator`外套了一层，没有拓展的功能可以省略
// type ReaderIterator struct {
// 	iter *engine_util.BadgerIterator
// }

// func NewReaderIterator(iter *engine_util.BadgerIterator) *ReaderIterator {
// 	return &ReaderIterator{
// 		iter: iter,
// 	}
// }

// func (ri *ReaderIterator) Item() engine_util.DBItem {
// 	return ri.iter.Item()
// }

// func (ri *ReaderIterator) Close() {
// 	ri.iter.Close()
// }

// func (ri *ReaderIterator) Valid() bool {
// 	return ri.iter.Valid()
// }

// func (ri *ReaderIterator) Next() {
// 	ri.iter.Next()
// }

// func (ri *ReaderIterator) Seek(key []byte) {
// 	ri.iter.Seek(key)
// }
