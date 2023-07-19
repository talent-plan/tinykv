package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"fmt"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	storeEngine *engine_util.Engines
	config *config.Config
}
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// 配置初始化,更新conf
	kvPath := conf.DBPath + "/kv_data"
	raftPath := conf.DBPath + "/raft_data"
	kvEngine := engine_util.CreateDB(kvPath,false)
	var raftEngine * badger.DB
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}
	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{
		storeEngine:   engines,
		config:		   conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.storeEngine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.storeEngine.Kv.NewTransaction(false)
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _ , modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.storeEngine.Kv,modify.Cf(),modify.Key(),modify.Value())
			if err != nil {
				fmt.Printf("update with error: %v\n", err)
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.storeEngine.Kv,modify.Cf(),modify.Key())
			if err != nil {
				fmt.Printf("delete with error: %v\n", err)
				return err
			}
		default :
			fmt.Printf("illegal operation in Write handler")
			return nil
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
