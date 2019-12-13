package main

import (
	"context"
	"encoding/binary"
	"flag"
	"google.golang.org/grpc/keepalive"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

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
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "", "config file path")
	pdAddr     = flag.String("pd", "", "pd address")
	storeAddr  = flag.String("addr", "", "store address")
)

var (
	gitHash = "None"
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30

	subPathRaft = "raft"
	subPathKV   = "kv"
)

func main() {
	flag.Parse()
	conf := loadConfig()
	if *pdAddr != "" {
		conf.PDAddr = *pdAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	safePoint := &tikv.SafePoint{}
	log.Infof("conf %v", conf)
	db := createDB(subPathKV, safePoint, &conf.Engine)
	bundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(8 << 20),
		RollbackStore: lockstore.NewMemStore(256 << 10),
	}

	pdClient, err := pd.NewClient(strings.Split(conf.PDAddr, ","), "")
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
	err = store.StartDeadlockDetection(context.Background(), pdClient, innerServer, conf.Raft)
	if err != nil {
		log.Fatal("StartDeadlockDetection error=%v", err)
	}

	tikvServer := tikv.NewServer(regionManager, store, innerServer)

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(grpcInitialWindowSize),
		grpc.InitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	deadlock.RegisterDeadlockServer(grpcServer, tikvServer)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)
	go func() {
		log.Infof("listening on %v", conf.HttpAddr)
		http.HandleFunc("/status", func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
		err := http.ListenAndServe(conf.HttpAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
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
	config.SplitCheck.RegionMaxKeys = uint64(conf.Coprocessor.RegionMaxKeys)
	config.SplitCheck.RegionSplitKeys = uint64(conf.Coprocessor.RegionSplitKeys)

	raftDB := createDB(subPathRaft, nil, &conf.Engine)
	meta, err := bundle.LockStore.LoadFromFile(filepath.Join(kvPath, raftstore.LockstoreFileName))
	if err != nil {
		log.Fatal(err)
	}
	var offset uint64
	if meta != nil {
		offset = binary.LittleEndian.Uint64(meta)
	}
	err = raftstore.RestoreLockStore(offset, bundle, raftDB)
	if err != nil {
		log.Fatal(err)
	}

	engines := raftstore.NewEngines(bundle, raftDB, kvPath, raftPath)

	innerServer := raftstore.NewRaftInnerServer(engines, config)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(bundle, dbPath, safePoint, raftstore.NewDBWriter(router), pdClient)
	rm := tikv.NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
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
	store := tikv.NewMVCCStore(bundle, conf.Engine.DBPath, safePoint, tikv.NewDBWriter(bundle, safePoint), pdClient)
	store.DeadlockDetectSvr.ChangeRole(tikv.Leader)
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
	if subPath == subPathRaft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
	}
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.ValueLogLoadingMode = options.FileIO
	opts.MaxTableSize = conf.MaxTableSize
	opts.NumMemtables = conf.NumMemTables
	opts.NumLevelZeroTables = conf.NumL0Tables
	opts.NumLevelZeroTablesStall = conf.NumL0TablesStall
	opts.SyncWrites = conf.SyncWrite
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
