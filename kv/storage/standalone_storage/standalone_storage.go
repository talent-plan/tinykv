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
	db    *badger.DB
	Debug bool
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB("tmp", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	s.Debug = true
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.Debug = false
	return nil
}

type StandAloneStorageReader struct {
	inner     *StandAloneStorage
	iterCount int
	iters     []*engine_util.BadgerIterator
	txns      []*badger.Txn
}

func (r StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	r.iterCount += 1
	_value, _ := engine_util.GetCF(r.inner.db, cf, key)
	return _value, nil
}
func (r StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	r.iterCount += 1
	var txn *badger.Txn
	txn = r.inner.db.NewTransaction(false)
	_iter := engine_util.NewCFIterator(cf, txn)
	r.iters = append(r.iters, _iter)
	r.txns = append(r.txns, txn)
	return _iter
}
func (r StandAloneStorageReader) Close() {

	for i, _ := range r.iters {
		r.iters[i].Close()
		r.txns[i].Discard()
		r.iterCount--
	}
	r.iters = []*engine_util.BadgerIterator{}
	r.txns = []*badger.Txn{}
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	sr := StandAloneStorageReader{s, 0, []*engine_util.BadgerIterator{}, []*badger.Txn{}}
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, data.Cf, data.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
