package main

import (
	"encoding/binary"
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	pdAddr           = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
	storeAddr        = flag.String("store-addr", "127.0.0.1:9191", "store address")
	httpAddr         = flag.String("http-addr", "127.0.0.1:9291", "http address")
	dbPath           = flag.String("db-path", "/tmp/badger", "Directory to store the data in. Should exist and be writable.")
	vlogPath         = flag.String("vlog-path", "", "Directory to store the value log in. can be the same as db-path.")
	vlogFileSize     = flag.Int64("vlog-file-size", 1024*1024*1024, "value log file size")
	valThreshold     = flag.Int("value-threshold", 20, "If value size >= this threshold, only store value offsets in tree.")
	regionSize       = flag.Int64("region-size", 96*1024*1024, "Average region size.")
	logLevel         = flag.String("L", "info", "log level")
	tableLoadingMode = flag.String("table-loading-mode", "memory-map", "How should LSM tree be accessed. (memory-map/load-to-ram)")
	maxTableSize     = flag.Int64("max-table-size", 64<<20, "Each table (or file) is at most this size.")
	numMemTables     = flag.Int("num-mem-tables", 3, "Maximum number of tables to keep in memory, before stalling.")
	numL0Table       = flag.Int("num-level-zero-tables", 3, "Maximum number of Level 0 tables before we start compacting.")
	syncWrites       = flag.Bool("sync-write", true, "Sync all writes to disk. Setting this to true would slow down data loading significantly.")
	maxProcs         = flag.Int("max-procs", 0, "Max CPU cores to use, set 0 to use all CPU cores in the machine.")
	raft             = flag.Bool("raft", false, "enable raft")
	raftWorkerCnt    = flag.Uint64("raft-worker-cnt", 2, "number of raft workers")
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
	runtime.GOMAXPROCS(*maxProcs)
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(*logLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	go func() {
		log.Infof("listening on %v", *httpAddr)
		err := http.ListenAndServe(*httpAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	safePoint := &tikv.SafePoint{}
	db := createDB("kv", safePoint)
	bundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(8 << 20),
		RollbackStore: lockstore.NewMemStore(256 << 10),
	}

	pdClient, err := pd.NewClient(*pdAddr, "")
	if err != nil {
		log.Fatal(err)
	}

	var (
		innerServer   tikv.InnerServer
		store         *tikv.MVCCStore
		regionManager tikv.RegionManager
	)
	if *raft {
		innerServer, store, regionManager = setupRaftInnerServer(bundle, safePoint, pdClient)
	} else {
		innerServer, store, regionManager = setupStandAlongInnerServer(bundle, safePoint, pdClient)
	}

	tikvServer := tikv.NewServer(regionManager, store, innerServer)

	grpcServer := grpc.NewServer(
		grpc.InitialWindowSize(grpcInitialWindowSize),
		grpc.InitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	l, err := net.Listen("tcp", *storeAddr)
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

func setupRaftInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, pdClient pd.Client) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
	kvPath := filepath.Join(*dbPath, "kv")
	raftPath := filepath.Join(*dbPath, "raft")
	snapPath := filepath.Join(*dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	config := raftstore.NewDefaultConfig()
	config.Addr = *storeAddr
	config.SnapPath = snapPath
	config.RaftWorkerCnt = *raftWorkerCnt

	raftDB := createDB("raft", nil)
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
	store := tikv.NewMVCCStore(bundle, *dbPath, safePoint, raftstore.NewDBWriter(router))
	rm := tikv.NewRaftRegionManager(storeMeta, router)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func setupStandAlongInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, pdClient pd.Client) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
	regionOpts := tikv.RegionOptions{
		StoreAddr:  *storeAddr,
		PDAddr:     *pdAddr,
		RegionSize: *regionSize,
	}

	innerServer := tikv.NewStandAlongInnerServer(bundle)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(bundle, *dbPath, safePoint, tikv.NewDBWriter(bundle, safePoint))
	rm := tikv.NewStandAloneRegionManager(bundle.DB, regionOpts, pdClient)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func createDB(subPath string, safePoint *tikv.SafePoint) *badger.DB {
	opts := badger.DefaultOptions
	opts.NumCompactors = 1
	opts.ValueThreshold = *valThreshold
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(*dbPath, subPath)
	if *vlogPath != "" {
		opts.ValueDir = filepath.Join(*dbPath, subPath)
	} else {
		opts.ValueDir = opts.Dir
	}
	if *tableLoadingMode == "memory-map" {
		opts.TableLoadingMode = options.MemoryMap
	}
	opts.ValueLogFileSize = *vlogFileSize
	opts.ValueLogLoadingMode = options.FileIO
	opts.MaxTableSize = *maxTableSize
	opts.NumMemtables = *numMemTables
	opts.NumLevelZeroTables = *numL0Table
	opts.NumLevelZeroTablesStall = opts.NumLevelZeroTables + 5
	opts.SyncWrites = *syncWrites
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
