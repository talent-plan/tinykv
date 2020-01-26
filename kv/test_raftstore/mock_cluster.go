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
	id       uint64
	trans    *MockTransport
	pdClient pd.Client
	count    int
	engines  map[uint64]*engine_util.Engines
	nodes    map[uint64]*raftstore.Node
}

func NewMockCluster(id uint64, count int, pdClient pd.Client) MockCluster {
	trans := NewMockTransport()
	return MockCluster{
		id:       id,
		trans:    &trans,
		count:    count,
		pdClient: pdClient,
		engines:  make(map[uint64]*engine_util.Engines),
		nodes:    make(map[uint64]*raftstore.Node),
	}
}

func (c *MockCluster) Start() error {
	regionID := uint64(1)
	ctx := context.TODO()
	firstRegion := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: raftstore.InitEpochVer,
			ConfVer: raftstore.InitEpochConfVer,
		},
	}

	for storeID := uint64(1); storeID <= uint64(c.count); storeID++ {
		dbPath, err := ioutil.TempDir("", "test-raftstore")
		if err != nil {
			return err
		}
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
		engine := engine_util.NewEngines(kvdb, raftDB, kvPath, raftPath)

		c.engines[storeID] = engine

		var wg sync.WaitGroup
		pdWorker := worker.NewWorker("pd-worker", &wg)

		router, batchSystem := raftstore.CreateRaftBatchSystem(raftConf)
		raftRouter := raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
		snapManager := snap.NewSnapManager(raftConf.SnapPath)
		node := raftstore.NewNode(batchSystem, &metapb.Store{}, raftConf, c.pdClient)

		err = raftstore.BootstrapStore(engine, c.id, storeID)
		if err != nil {
			return err
		}

		peerID := storeID
		peer := &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		}
		_, err = raftstore.PrepareBootstrap(engine, storeID, regionID, peerID)
		if err != nil {
			return err
		}
		firstRegion.Peers = append(firstRegion.Peers, peer)

		err = node.Start(ctx, engine, c.trans, snapManager, pdWorker, raftRouter)
		if err != nil {
			return err
		}

		c.nodes[storeID] = node
	}

	_, err := c.nodes[1].BootstrapCluster(ctx, c.engines[1], firstRegion)
	if err != nil {
		return err
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
