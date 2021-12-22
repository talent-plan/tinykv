package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	_ "github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)
// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	storage :=&StandAloneStorage{
		engine: nil,
		conf: conf,
	}
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1)
	path := s.conf.DBPath
	kvEngine := engine_util.CreateDB(path,false)
	//raftEngine := engine_util.CreateDB(path,false)
	if kvEngine == nil {
		return fmt.Errorf("create StandAloneStorage failed")
	}
	s.engine = engine_util.NewEngines(kvEngine,nil,path,"")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engine.Kv.Close()
	return err
}



func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := &engine_util.WriteBatch{}
	writeBatch.Reset()
	for _,epoch := range batch {
		writeBatch.SetCF(epoch.Cf(),epoch.Key(),epoch.Value())
	}
	err := s.engine.WriteKV(writeBatch)
	return err
}


func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 一个Reader对于一个txn
	txn := s.engine.Kv.NewTransaction(false)
	r := NewStandaloneReader(txn)
	return r, nil
}


type StandaloneReader struct{
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *StandaloneReader{
	return &StandaloneReader{
		txn:txn,
	}
}

func (r *StandaloneReader)GetCF(cf string, key []byte) ([]byte, error){
	val,err:= engine_util.GetCFFromTxn(r.txn,cf,key)
	if err == badger.ErrKeyNotFound{
		return nil ,nil
	}
	return val,err
}

func (r *StandaloneReader)IterCF(cf string) engine_util.DBIterator{
	// read-only
	return engine_util.NewCFIterator(cf,r.txn)
}

func (r *StandaloneReader)Close(){
	r.txn.Discard()
}