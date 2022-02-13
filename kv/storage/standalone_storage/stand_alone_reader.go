package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	db   *badger.DB
	txn  *badger.Txn
	iter *engine_util.BadgerIterator
}

func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(reader.db, cf, key)
}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	reader.txn = reader.db.NewTransaction(false)
	reader.iter = engine_util.NewCFIterator(cf, reader.txn)
	return reader.iter
}

func (reader *StandAloneReader) Close() {
	if reader.iter != nil {
		reader.iter.Close()
	}
	if reader.txn != nil {
		reader.txn.Discard()
	}
}
