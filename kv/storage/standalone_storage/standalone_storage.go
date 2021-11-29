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
	Conf config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{*conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &badgerReader{s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	opts := badger.DefaultOptions
	opts.Dir = s.Conf.DBPath
	opts.ValueDir = s.Conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	tx := db.NewTransaction(true)
	defer func() {
		tx.Discard()
		db.Close()
	}()
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			tx.Set(makeKey(data.Cf, data.Key), data.Value)
		case storage.Delete:
			tx.Delete(makeKey(data.Cf, data.Key))
		}
	}
	tx.Commit()
	return nil
}

type badgerReader struct {
	inner *StandAloneStorage
	//iterCount int
}

func makeKey(cf string, key []byte) []byte {
	r := make([]byte, 0, len(cf)+len(key)+1)
	for i := 0; i < len(cf); i++ {
		r = append(r, cf[i])
	}
	r = append(r, '_')
	for i := 0; i < len(key); i++ {
		r = append(r, key[i])
	}
	return r
}

func (mr *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	opts := badger.DefaultOptions
	opts.Dir = mr.inner.Conf.DBPath
	opts.ValueDir = mr.inner.Conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	value, err := tx.Get(makeKey(cf, key))
	if err != nil {
		return nil, err
	}
	if valueCopy, err := value.ValueCopy(nil); err == nil {
		return valueCopy, nil
	} else {
		return nil, err
	}
}

func (mr *badgerReader) IterCF(cf string) engine_util.DBIterator {
	opts := badger.DefaultOptions
	opts.Dir = mr.inner.Conf.DBPath
	opts.ValueDir = mr.inner.Conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil
	}
	defer db.Close()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	opt := badger.DefaultIteratorOptions
	it := tx.NewIterator(opt)
	return &badgerIter{it}
}

func (mr *badgerReader) Close() {
	/*if mr.iterCount > 0 {
		panic("Unclosed iterator")
	}*/
}

type badgerIter struct {
	iterator *badger.Iterator
}

func (it *badgerIter) Item() engine_util.DBItem {
	return it.iterator.Item()
}
func (it *badgerIter) Valid() bool {
	return it.Item().Key() != nil
}
func (it *badgerIter) Next() {
	it.Next()
}
func (it *badgerIter) Seek(key []byte) {
	it.iterator.Seek(key)
}

func (it *badgerIter) Close() {
	it.iterator.Close()
}
