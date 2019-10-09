package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "", "config file path")
)

var (
	gitHash = "None"
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

func main() {
	flag.Parse()
	conf := loadConfig()
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	go func() {
		log.Infof("listening on %v", conf.HttpAddr)
		err := http.ListenAndServe(conf.HttpAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	safePoint := &tikv.SafePoint{}
	db := createDB("kv", safePoint, &conf.Engine)
	bundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(8 << 20),
		RollbackStore: lockstore.NewMemStore(256 << 10),
	}

	pdClient, err := pd.NewClient(conf.PDAddr, "")
	if err != nil {
		log.Fatal(err)
	}

	var (
		innerServer   tikv.InnerServer
		store         *tikv.MVCCStore
		regionManager tikv.RegionManager
	)
	if conf.Raft {
		innerServer, store, regionManager = setupRaftInnerServer(bundle, safePoint, pdClient, conf)
	} else {
		innerServer, store, regionManager = setupStandAlongInnerServer(bundle, safePoint, pdClient, conf)
	}

	tikvServer := tikv.NewServer(regionManager, store, innerServer)

	grpcServer := grpc.NewServer(
		grpc.InitialWindowSize(grpcInitialWindowSize),
		grpc.InitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	l, err := net.Listen("tcp", conf.StoreAddr)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)
	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal(err)
	}
	tikvServer.Stop()
	log.Info("Server stopped.")

	err = store.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Store closed.")

	if err = regionManager.Close(); err != nil {
		log.Fatal(err)
	}

	err = innerServer.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

func loadConfig() *config.Config {
	conf := config.DefaultConf
	if *configPath != "" {
		_, err := toml.DecodeFile(*configPath, &conf)
		if err != nil {
			panic(err)
		}
	}
	return &conf
}

func setupRaftInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, pdClient pd.Client, conf *config.Config) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
	dbPath := conf.Engine.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	config := raftstore.NewDefaultConfig()
	config.Addr = conf.StoreAddr
	config.SnapPath = snapPath
	config.RaftWorkerCnt = conf.RaftWorkers

	raftDB := createDB("raft", nil, &conf.Engine)
	meta, err := bundle.LockStore.LoadFromFile(filepath.Join(kvPath, raftstore.LockstoreFileName))
	if err != nil {
		log.Fatal(err)
	}
	if meta != nil {
		offset := binary.LittleEndian.Uint64(meta)
		err = raftstore.RestoreLockStore(offset, bundle, raftDB)
		if err != nil {
			log.Fatal(err)
		}
		log.Info("restored lock store from offset", offset)
	}

	engines := raftstore.NewEngines(bundle, raftDB, kvPath, raftPath)

	innerServer := raftstore.NewRaftInnerServer(engines, config)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(bundle, dbPath, safePoint, raftstore.NewDBWriter(router))
	rm := tikv.NewRaftRegionManager(storeMeta, router)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func setupStandAlongInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, pdClient pd.Client, conf *config.Config) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
	regionOpts := tikv.RegionOptions{
		StoreAddr:  conf.StoreAddr,
		PDAddr:     conf.PDAddr,
		RegionSize: conf.RegionSize,
	}

	innerServer := tikv.NewStandAlongInnerServer(bundle)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(bundle, conf.Engine.DBPath, safePoint, tikv.NewDBWriter(bundle, safePoint))
	rm := tikv.NewStandAloneRegionManager(bundle.DB, regionOpts, pdClient)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func createDB(subPath string, safePoint *tikv.SafePoint, conf *config.Engine) *badger.DB {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.TableLoadingMode = options.MemoryMap
	fmt.Println("vlog file size", conf.VlogFileSize)
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.ValueLogLoadingMode = options.FileIO
	opts.MaxTableSize = conf.MaxTableSize
	opts.NumMemtables = conf.NumMemTables
	opts.NumLevelZeroTables = conf.NumL0Tables
	opts.NumLevelZeroTablesStall = conf.NumL0TablesStall
	opts.SyncWrites = conf.SyncWrite
	opts.TableBuilderOptions.EnableHashIndex = conf.HashIndex
	if safePoint != nil {
		opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sigCh
		log.Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}
