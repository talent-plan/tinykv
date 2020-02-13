package test_raftstore

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type Filter interface {
	Before(msgs *raft_serverpb.RaftMessage) bool
	After()
}

type Simulator interface {
	RunNode(raftConf *tikvConf.Config, engine *engine_util.Engines, ctx context.Context) error
	AddSendFilter(storeID uint64, filter Filter)
	AddReceiveFilter(storeID uint64, filter Filter)
	ClearSendFilters(storeID uint64)
	ClearReceiveFilters(storeID uint64)
	StopNodes()
}

type Cluster struct {
	clusterID uint64
	pdClient  pd.Client
	count     int
	engines   []*engine_util.Engines
	simulator Simulator
}

func NewCluster(clusterID uint64, count int, pdClient pd.Client) Cluster {
	simulator := NewNodeCluster(pdClient)
	return Cluster{
		clusterID: clusterID,
		count:     count,
		pdClient:  pdClient,
		simulator: &simulator,
	}
}

func (c *Cluster) Start() error {
	ctx := context.TODO()

	for storeID := uint64(1); storeID <= uint64(c.count); storeID++ {
		dbPath, err := ioutil.TempDir("", "test-raftstore")
		if err != nil {
			return err
		}
		kvPath := filepath.Join(dbPath, "kv")
		raftPath := filepath.Join(dbPath, "raft")
		snapPath := filepath.Join(dbPath, "snap")

		err = os.MkdirAll(kvPath, os.ModePerm)
		if err != nil {
			return err
		}
		err = os.MkdirAll(raftPath, os.ModePerm)
		if err != nil {
			return err
		}
		err = os.Mkdir(snapPath, os.ModePerm)
		if err != nil {
			return err
		}

		conf := config.DefaultConf
		raftConf := tikvConf.NewDefaultConfig()
		raftConf.SnapPath = snapPath

		raftDB := createDB("raft", &conf.Engine)
		kvdb := createDB("kv", &conf.Engine)

		engine := engine_util.NewEngines(kvdb, raftDB, kvPath, raftPath)
		err = raftstore.BootstrapStore(engine, c.clusterID, storeID)
		if err != nil {
			return err
		}
		c.engines = append(c.engines, engine)

		err = c.simulator.RunNode(raftConf, engine, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func createDB(subPath string, conf *config.Engine) *badger.DB {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	if subPath == "raft" {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
	}
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.MaxTableSize = conf.MaxTableSize
	opts.NumMemtables = conf.NumMemTables
	opts.NumLevelZeroTables = conf.NumL0Tables
	opts.NumLevelZeroTablesStall = conf.NumL0TablesStall
	opts.SyncWrites = conf.SyncWrite
	opts.MaxCacheSize = conf.BlockCacheSize
	opts.TableBuilderOptions.SuRFStartLevel = conf.SurfStartLevel
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
