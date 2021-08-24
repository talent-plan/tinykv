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
	conf   *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db := engine_util.CreateDB(s.conf.DBPath, s.conf.Raft)
	engine := engine_util.NewEngines(db, db, s.conf.DBPath, s.conf.DBPath)
	s.engine = engine
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	sr := &standAloneReader{
		txn: s.engine.Kv.NewTransaction(false),
	}
	return sr, nil
}

type standAloneReader struct {
	txn *badger.Txn
}

func (s standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(s.txn, cf, key)
}

func (s standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standAloneReader) Close() {
	s.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	batchWrites := &engine_util.WriteBatch{}
	for _, b := range batch {
		batchWrites.SetCF(b.Cf(), b.Key(), b.Value())
	}
	return s.engine.WriteKV(batchWrites)
}
