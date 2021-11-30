package standalone_storage

import (
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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &BadgerReader{s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	//tx := s.db.NewTransaction(true)
	//defer tx.Discard()
	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			writeBatch.DeleteCF(data.Cf, data.Key)
		}
	}
	writeBatch.WriteToDB(s.db)
	//tx.Commit()
	return nil
}

type BadgerReader struct {
	inner *StandAloneStorage
}

func (mr *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCF(mr.inner.db, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	} else {
		return value, err
	}
}

func (mr *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	tx := mr.inner.db.NewTransaction(false)
	//defer tx.Discard()
	return engine_util.NewCFIterator(cf, tx)
}

func (mr *BadgerReader) Close() {
}
