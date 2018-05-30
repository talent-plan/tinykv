package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/ngaut/faketikv/tikv"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	pdAddr           = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
	storeAddr        = flag.String("store-addr", "127.0.0.1:9191", "store address")
	httpAddr         = flag.String("http-addr", "127.0.0.1:9291", "http address")
	dbPath           = flag.String("db-path", "/tmp/badger", "Directory to store the data in. Should exist and be writable.")
	valThreshold     = flag.Int("value-threshold", 20, "If value size >= this threshold, only store value offsets in tree.")
	regionSize       = flag.Int64("region-size", 96*1024*1024, "Average region size.")
	logLevel         = flag.String("L", "info", "log level")
	tableLoadingMode = flag.String("table-loading-mode", "memory-map", "How should LSM tree be accessed. (memory-map/load-to-ram)")
	maxTableSize     = flag.Int64("max-table-size", 64<<20, "Each table (or file) is at most this size.")
	numMemTables     = flag.Int("num-mem-tables", 3, "Maximum number of tables to keep in memory, before stalling.")
	numL0Table       = flag.Int("num-level-zero-tables", 3, "Maximum number of Level 0 tables before we start compacting.")
	syncWrites       = flag.Bool("sync-write", true, "Sync all writes to disk. Setting this to true would slow down data loading significantly.")
)

var (
	gitHash = "None"
)

func main() {
	flag.Parse()
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(*logLevel)
	go http.ListenAndServe(*httpAddr, nil)

	opts := badger.DefaultOptions
	opts.ValueThreshold = *valThreshold
	opts.Dir = *dbPath
	opts.ValueDir = *dbPath
	if *tableLoadingMode == "memory-map" {
		opts.TableLoadingMode = options.MemoryMap
	}
	opts.ValueLogLoadingMode = options.FileIO
	opts.MaxTableSize = *maxTableSize
	opts.NumMemtables = *numMemTables
	opts.NumLevelZeroTables = *numL0Table
	opts.NumLevelZeroTablesStall = opts.NumLevelZeroTables + 5
	opts.SyncWrites = *syncWrites

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	ops := tikv.RegionOptions{
		StoreAddr:  *storeAddr,
		PDAddr:     *pdAddr,
		RegionSize: *regionSize,
	}
	rm := tikv.NewRegionManager(db, ops)
	tikvServer := tikv.NewServer(rm, db)
	grpcServer := grpc.NewServer()
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	l, err := net.Listen("tcp", *storeAddr)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(db, grpcServer)
	err = grpcServer.Serve(l)
	if err != nil {
		log.Error(err)
	}
}

func handleSignal(db *badger.DB, server *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sigCh
		log.Infof("Got signal [%s] to exit.", sig)
		err := db.Close()
		if err != nil {
			log.Error(err)
		} else {
			log.Info("DB closed.")
		}
		server.Stop()
	}()
}
