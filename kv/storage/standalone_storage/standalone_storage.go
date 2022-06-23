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
	db  *badger.DB
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	if conf.Raft {
		path := conf.DBPath + "/raft"
		DB := engine_util.CreateDB(path, true)
		return &StandAloneStorage{db: DB}
	} else {
		path := conf.DBPath + "/kv"
		DB := engine_util.CreateDB(path, false)
		return &StandAloneStorage{db: DB}
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

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		return nil, err
	}
	return val, err
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorage) Close() {
	s.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	rTxn := s.db.NewTransaction(true)
	return &StandAloneStorage{txn: rTxn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wTxn := s.db.NewTransaction(true)
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			mput := b.Data.(storage.Put)
			err := wTxn.Set(engine_util.KeyWithCF(mput.Cf, mput.Key), mput.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			mdelete := b.Data.(storage.Delete)
			err := wTxn.Delete(engine_util.KeyWithCF(mdelete.Cf, mdelete.Key))
			if err != nil {
				return err
			}
		}
	}
	err := wTxn.Commit()
	if err != nil {
		return err
	}
	return nil
}
