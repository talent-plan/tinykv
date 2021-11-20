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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath+"/standalone", conf.Raft)
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneReader{inner: s, txn: s.db.NewTransaction(true)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var errs []error
	for _, m := range batch {
		cf := m.Cf()
		key := m.Key()
		value := m.Value()
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, cf, key, value)
			errs = append(errs, err)
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, cf, key)
			errs = append(errs, err)
		}
	}

	return errs[0]

}

type standaloneReader struct {
	inner *StandAloneStorage
	iters []*engine_util.BadgerIterator
	txn   *badger.Txn
}

func (sr *standaloneReader) Close() {
	for _, it := range sr.iters {
		it.Close()
	}
	sr.txn.Discard()
}

// When the key doesn't exist, return nil for the value
func (sr *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	value, err := engine_util.GetCF(sr.inner.db, cf, key)
	// check whether the key exists
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err

}
func (sr *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	newIterator := engine_util.NewCFIterator(cf, sr.txn)
	sr.iters = append(sr.iters, newIterator)
	return newIterator
}
