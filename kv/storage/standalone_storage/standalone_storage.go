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
	kvDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		kvDB: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.kvDB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &badgerReader{s.kvDB.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := new(engine_util.WriteBatch)
	txn := s.kvDB.NewTransaction(true)
	defer txn.Discard()
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			writeBatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	return writeBatch.WriteToDB(s.kvDB)
}

type badgerReader struct {
	txn *badger.Txn
}

func (b *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	result, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return result, err
}

func (b *badgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *badgerReader) Close() {
	b.txn.Discard()
}
