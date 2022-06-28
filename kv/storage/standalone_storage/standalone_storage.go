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
	db   *badger.DB
	opts *badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	return &StandAloneStorage{
		opts: &opts,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := badger.Open(*s.opts)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db == nil {
		return nil
	}

	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) (err error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for i := range batch {
		switch batch[i].Data.(type) {
		case storage.Put:
			put := batch[i].Data.(storage.Put)
			err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
		case storage.Delete:
			deleter := batch[i].Data.(storage.Delete)
			err = txn.Delete(engine_util.KeyWithCF(deleter.Cf, deleter.Key))
		default:
		}

		switch err {
		case nil:
		case badger.ErrTxnTooBig:
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = s.db.NewTransaction(true)
		}
	}

	return txn.Commit()
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(db *badger.DB) *StandAloneReader {
	return &StandAloneReader{
		txn: db.NewTransaction(false),
	}
}

// When the key doesn't exist, return nil for the value
func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	switch err {
	case nil:
	case badger.ErrKeyNotFound:
		return nil, nil
	default:
		return nil, err
	}

	return item.Value()
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
