package engine_util

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/golang/protobuf/proto"
)

func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append([]byte(cf+"_"), key...))
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	return
}

func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) ([]byte, error) {
	item, err := txn.Get(append([]byte(cf+"_"), key...))
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	return val, err
}

func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

func GetMsg(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := GetValue(engine, key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func GetValueTxn(txn *badger.Txn, key []byte) ([]byte, error) {
	i, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	return i.Value()
}

func GetValue(engine *badger.DB, key []byte) ([]byte, error) {
	var result []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		result = val
		return err
	})
	return result, err
}

func PutMsg(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return PutValue(engine, key, val)
}

func PutValue(engine *badger.DB, key, val []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
