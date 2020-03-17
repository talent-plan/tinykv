package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB("kv", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := NewStorageReader(txn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			var err error
			switch modify.Type {
			case storage.ModifyTypePut:
				put := modify.Data.(storage.Put)
				err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			case storage.ModifyTypeDelete:
				delete := modify.Data.(storage.Delete)
				err = txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			default:
				err = errors.New("Unmatchable modify type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type StorageReader struct {
	txn *badger.Txn
}

func NewStorageReader(txn *badger.Txn) *StorageReader {
	return &StorageReader{txn}
}

func (storeReader *StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(storeReader.txn, cf, key)
}

func (storeReader *StorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, storeReader.txn)
}

func (storeReader *StorageReader) Close() {
	storeReader.txn.Discard()
}
