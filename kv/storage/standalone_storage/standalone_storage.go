package standalone_storage

import (
	"io/ioutil"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DB *badger.DB
	R  storage.StorageReader
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dir, err := ioutil.TempDir("/", conf.DBPath)
	if err != nil {
		panic(err)
	}
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{DB: db}
}

var _ storage.Storage = (*StandAloneStorage)(nil)

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.R != nil {
		s.R.Close()
	}
	return s.DB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.R != nil {
		return s.R, nil
	}
	reader := NewStandAloneStorageReader(*s)
	s.R = reader
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	b := new(engine_util.WriteBatch)
	for _, entry := range batch {
		b.SetCF(entry.Cf(), entry.Key(), entry.Value())
	}
	err := b.WriteToDB(s.DB)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type StandAloneStorageReader struct {
	Storage   StandAloneStorage
	CFIterMap map[string]engine_util.DBIterator
}

func NewStandAloneStorageReader(storage StandAloneStorage) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		Storage:   storage,
		CFIterMap: make(map[string]engine_util.DBIterator, 0),
	}
}

var _ storage.StorageReader = (*StandAloneStorageReader)(nil)

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.Storage.DB, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if iter, ok := r.CFIterMap[cf]; ok {
		return iter
	}
	iter := engine_util.NewCFIterator(cf, r.Storage.DB.NewTransaction(false))
	// 存入 reader 中，避免重复 new cf iter
	r.CFIterMap[cf] = iter
	return iter
}

func (r *StandAloneStorageReader) Close() {
	// Close all cf iterators
	for _, iter := range r.CFIterMap {
		iter.Close()
	}
	return
}
