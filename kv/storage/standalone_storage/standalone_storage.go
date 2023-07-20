package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// 底层的存储引擎
	// TODO:追一下util包下Engines的实现
	storeEngine *engine_util.Engines
	// 单机KV引擎的配置信息
	config *config.Config
}
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// 配置初始化，主要看config中需要更新哪些
	// 注意这里底层持久化是通过badger数据库实现的，（见指导手册）
	// 这里单机kv引擎只需要起一个badger数据库,持久化KV数据即可，raft模式下要起两个badger数据库，一个存KV数据，一个存Raft的MetaData(见engine_util.Engines结构体实现)
	kvPath := conf.DBPath + "/kv_data"
	raftPath := conf.DBPath + "/raft_data"
	var kvEngine * badger.DB
	// kvEngine := engine_util.CreateDB(kvPath,false)
	var raftEngine * badger.DB
	// if conf.Raft {
	// 	raftEngine = engine_util.CreateDB(raftPath, true)
	// }
	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{
		storeEngine:   engines,
		config:		   conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 调用start时才初始化数据库
	s.storeEngine.Kv = engine_util.CreateDB(s.storeEngine.KvPath,false)
	if s.config.Raft {
		s.storeEngine.Raft = engine_util.CreateDB(s.storeEngine.RaftPath,true)
	} else {
		s.storeEngine.Raft = engine_util.CreateDB(s.storeEngine.RaftPath,false)
	}
	// TODO:很奇怪CreateDB没有返回错误信息，这里只能返回nil了，细看一下badger数据库使用
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
		// TODO:追一下storage.Modify的实现，仿照他的断言方法实现CRUD，这里还是没太明白
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.storeEngine.Kv,modify.Cf(),modify.Key(),modify.Value())
			if err != nil {
				log.Fatal("write with error: %v\n", err)
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.storeEngine.Kv,modify.Cf(),modify.Key())
			if err != nil {
				log.Fatal("delete with error: %v\n", err)
				return err
			}
		default :
			log.Fatal("illegal operation in Write handler")
			return nil
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}
// 返回cf对应的列族
func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	// 无key情况，测试debug时会出现
	if err == badger.ErrKeyNotFound {
		log.Info("key not found when GetCFFrom Txn")
		return nil, nil
	}
	return value, err
}
// 返回一个DB迭代器
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
