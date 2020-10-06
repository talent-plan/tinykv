package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type badgerReader struct {
	db *badger.DB
	activeTxn *badger.Txn
}

func (b badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(b.db, cf, key)
}

func (b badgerReader) IterCF(cf string) engine_util.DBIterator {
	if b.activeTxn != nil {
		log.Fatal("Transaction already started")
	}
	b.activeTxn = b.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, b.activeTxn)
}

func (b badgerReader) Close() {
	if b.activeTxn != nil {
		b.activeTxn.Discard()
		b.activeTxn = nil
	}
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbPath string
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		dbPath: conf.DBPath,
		db: nil,
	}
}

func (s *StandAloneStorage) Start() error {
	s.db = engine_util.CreateDB(s.dbPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database has not been initialized")
	}
	return badgerReader{db: s.db}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var err error

	for _, item := range batch {
		err = engine_util.PutCF(s.db, item.Cf(), item.Key(), item.Value())
		if err != nil {
			return err
		}
	}
	return nil
}
