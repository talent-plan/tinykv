package standalone_storage

import (
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
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvDBPath, rfDBPath := path.Join(conf.DBPath, "kv"), path.Join(conf.DBPath, "rf")
	kvDB := engine_util.CreateDB(kvDBPath, false)
	rfDB := engine_util.CreateDB(rfDBPath, true)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, rfDB, kvDBPath, rfDBPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for i := 0; i < len(batch); i++ {
		switch batch[i].Data.(type) {
		case storage.Put:
			data := batch[i].Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, data.Cf, data.Key, data.Value)
			if err != nil {return err}
		case storage.Delete:
			data := batch[i].Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, data.Cf, data.Key)
			if err != nil {return err}
		}
	}
	return nil
}
