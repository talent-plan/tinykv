package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	reader := NewBadgerReader(txn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			var err error
			switch op.Data.(type) {
			case storage.Put:
				put := op.Data.(storage.Put)
				err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			case storage.Delete:
				delete := op.Data.(storage.Delete)
				err = txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			default:
				err = errors.New("Unsupported modify type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type BadgerReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(b.txn, cf, key)
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard()
}
