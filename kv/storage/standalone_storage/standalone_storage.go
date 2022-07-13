package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvPath   string
	badgerDB *badger.DB
	conf     *config.Config
}

type StandAloneStorageReader struct {
	badgerTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	os.MkdirAll(kvPath, os.ModePerm)
	db := engine_util.CreateDB(kvPath, false)
	return &StandAloneStorage{kvPath: kvPath, badgerDB: db, conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.badgerDB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.badgerDB.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.badgerDB.NewTransaction(true)
	for _, operation := range batch {
		switch operation.Data.(type) {
		case storage.Put:
			put := operation.Data.(storage.Put)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := operation.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	return err
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{badgerTxn: txn}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.badgerTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		panic("wrong when getting data")
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.badgerTxn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.badgerTxn.Discard()
}
