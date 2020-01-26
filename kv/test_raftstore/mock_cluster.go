package test_raftstore

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type MockCluster struct {
	trans    *MockTransport
	pdClient pd.Client
	count    int
	nodes    map[uint64]*raftstore.Node
}

func NewMockCluster(pdClient pd.Client, count int) MockCluster {
	trans := NewMockTransport()
	return MockCluster{
		trans:    &trans,
		count:    count,
		pdClient: pdClient,
	}
}

func (c *MockCluster) Start() error {
	for nodeID := 0; nodeID < c.count; nodeID++ {
		err := c.addNode(uint64(nodeID))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *MockCluster) addNode(nodeID uint64) error {
	dbPath, err := ioutil.TempDir("", "test-raftstore")
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	conf := config.DefaultConf
	raftConf := tikvConf.NewDefaultConfig()
	raftConf.SnapPath = snapPath

	raftDB := createDB("raft", &conf.Engine)
	kvdb := createDB("kv", &conf.Engine)
	engines := engine_util.NewEngines(kvdb, raftDB, kvPath, raftPath)

	var wg sync.WaitGroup
	pdWorker := worker.NewWorker("pd-worker", &wg)

	router, batchSystem := raftstore.CreateRaftBatchSystem(raftConf)
	raftRouter := raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	snapManager := snap.NewSnapManager(raftConf.SnapPath)
	node := raftstore.NewNode(batchSystem, &metapb.Store{}, raftConf, c.pdClient)

	err = node.Start(context.TODO(), engines, c.trans, snapManager, pdWorker, raftRouter)
	if err != nil {
		return err
	}

	c.nodes[nodeID] = node

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
