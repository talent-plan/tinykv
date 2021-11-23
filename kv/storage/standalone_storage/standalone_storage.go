package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

type StandAloneReader struct{
	kvTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// generate the path of badger.DB
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath,"raft")
	// generate engines
	kvEngine := engine_util.CreateDB(kvPath,false)
	raftEngine := engine_util.CreateDB(raftPath,true)

	return &StandAloneStorage{
		engine : engine_util.NewEngines(kvEngine,raftEngine,kvPath,raftPath),
		config : conf,
	}
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader{
	return &StandAloneReader{
		kvTxn: txn,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()		//Pass the error to the caller
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)),nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch{
		// Refer: kv/storage/modify.go
		switch b.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.engine.Kv,b.Cf(),b.Key(),b.Value());err!=nil{
				return err
			}
		case storage.Delete:
			if err:= engine_util.DeleteCF(s.engine.Kv,b.Cf(),b.Key());err!=nil{
				return err
			}
		}
	}
	return nil
}

func (r *StandAloneReader)GetCF(cf string, key []byte) ([]byte, error){
	value , err := engine_util.GetCFFromTxn(r.kvTxn,cf,key)
	if err == badger.ErrKeyNotFound{
		return nil,nil
	}
	return value, err
}

func (r *StandAloneReader)IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf,r.kvTxn)
}

func (r *StandAloneReader) Close() {
	r.kvTxn.Discard()
}
