package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	dbPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := filepath.Join(conf.DBPath, "StandAloneStorage")
	return &StandAloneStorage{
		nil,
		dbPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// Create DB when it start
	s.db := engine_util.CreateDB(s.dbPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// setup a transaction, then return a struct

	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			writeBatch.DeleteCF(data.Cf, data.Key)
		}
	}
	return writeBatch.WriteToDB(s.db)
}

type StandAloneStorageReader struct {
	tnx *badger.Txn
}

func (a *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(a.tnx, cf, key)
	if err != nil && err == badger.ErrKeyNotFound {
		return nil, err
	}
	return val, err
}

func (a *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, a.tnx)
}

func (a *StandAloneStorageReader) Close() {
	a.tnx.Discard()
}
