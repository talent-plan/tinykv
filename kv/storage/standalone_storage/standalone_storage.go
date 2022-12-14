package standalone_storage

import (
	"fmt"

	"github.com/jmhodges/levigo"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db       *levigo.DB
	roptions *levigo.ReadOptions
	woptions *levigo.WriteOptions
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbName := conf.DBPath
	opt := levigo.NewOptions()
	ropt := levigo.NewReadOptions()
	wopt := levigo.NewWriteOptions()
	opt.SetCreateIfMissing(true)
	opt.SetWriteBufferSize(67108864)
	policy := levigo.NewBloomFilter(10)
	opt.SetFilterPolicy(policy)
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return nil
	}
	s := &StandAloneStorage{
		db,
		ropt,
		wopt,
	}
	return s
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

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		s.db,
		s.roptions,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			if err := s.db.Put(s.woptions, engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			if err := s.db.Delete(s.woptions, engine_util.KeyWithCF(del.Cf, del.Key)); err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	db       *levigo.DB
	roptions *levigo.ReadOptions
}

func (sReader *StandAloneStorageReader) Close() {
	return
}

func (sReader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := sReader.db.Get(sReader.roptions, engine_util.KeyWithCF(cf, key))

	return val, err
}

func (sReader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewLDBIterator(cf, sReader.db, sReader.roptions)
}

// func (sReader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
// value, err := sReader.db.Get(sReader.)

// v, e := engine_util.GetCF(sReader.db, cf, key)
// if e == badger.ErrKeyNotFound {
// 	return nil, nil
// }
// return v, e
// }
// func (sReader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
// txn := sReader.db.NewTransaction(false)
// return engine_util.NewCFIterator(cf, txn)
// }
