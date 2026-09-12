package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		engine_util.Engines{
			Kv:     engine_util.CreateDB(conf.DBPath, conf.Raft),
			KvPath: conf.DBPath,
		},
	}
}

func (s *StandAloneStorage) Start() error {
	// в воркфлоу база уже запущена. можно сделать иначе: в конструкторе сохранить конфиг, а здесь стартануть базу
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.Kv.NewTransaction(false) // транзакция в данном случае и есть снэпшот
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var wb engine_util.WriteBatch

	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}

	return wb.WriteToDB(s.Kv)
}

type standAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *standAloneStorageReader {
	return &standAloneStorageReader{
		txn: txn,
	}
}

func (s *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (s *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *standAloneStorageReader) Close() {
	s.txn.Discard()
}
