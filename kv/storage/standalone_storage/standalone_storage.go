package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(s.txn, cf, key)
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf, nil}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.db.Close(); err != nil {
		return err
	}
	return os.RemoveAll(s.conf.DBPath)
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneReader{txn}, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			return s.db.Update(
				func(txn *badger.Txn) error {
					return txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value())
				},
			)
		case storage.Delete:
			return s.db.Update(
				func(txn *badger.Txn) error {
					return txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), nil)
				},
			)
		}
	}
	return nil
}
