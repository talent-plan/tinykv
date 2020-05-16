package standalone_storage

import (
	"io/ioutil"
	"os"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all collections is stored locally.
type StandAloneStorage struct {
	badgerDB *badger.DB
}

// First, we should new a stand alone storage based on conf
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	initializeDir(conf.DBPath)
	dir, err := ioutil.TempDir(conf.DBPath, "")
	if err != nil {
		panic(err)
	}
	log.Infof("create tmp dir:%s", dir)

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{
		badgerDB: db,
	}
}

// create a path if it does't exists
func initializeDir(path string) {
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		os.MkdirAll(path, 0700)
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{storage: s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// badger 有 batch 方法
	for _, b := range batch {
		key := createIndexKey(b.Cf(), string(b.Key()))
		switch b.Data.(type) {
		case storage.Put:
			err := s.badgerDB.Update(func(txn *badger.Txn) error {
				err := txn.Set([]byte(key), b.Value())
				return err
			})
			if err != nil {
				return err
			}
		case storage.Delete:
			err := s.badgerDB.Update(func(txn *badger.Txn) error {
				err := txn.Delete([]byte(key))
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}
