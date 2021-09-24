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
	instance *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		instance: engine_util.CreateDB(conf.DBPath, conf.Raft),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.instance.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.instance.NewTransaction(false)
	return &StorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.instance.NewTransaction(true)
	for _, item := range batch {
		switch item.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(item.Cf(), item.Key()), item.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(item.Cf(), item.Key())); err != nil {
				return err
			}
		}
	}

	defer txn.Discard()
	return txn.Commit()
}


