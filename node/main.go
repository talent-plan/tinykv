package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/tikv"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	pdAddr           = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
	storeAddr        = flag.String("store-addr", "127.0.0.1:9191", "store address")
	httpAddr         = flag.String("http-addr", "127.0.0.1:9291", "http address")
	dbPath           = flag.String("db-path", "/tmp/badger", "Directory to store the data in. Should exist and be writable.")
	vlogPath         = flag.String("vlog-path", "", "Directory to store the value log in. can be the same as db-path.")
	valThreshold     = flag.Int("value-threshold", 20, "If value size >= this threshold, only store value offsets in tree.")
	regionSize       = flag.Int64("region-size", 96*1024*1024, "Average region size.")
	logLevel         = flag.String("L", "info", "log level")
	tableLoadingMode = flag.String("table-loading-mode", "memory-map", "How should LSM tree be accessed. (memory-map/load-to-ram)")
	maxTableSize     = flag.Int64("max-table-size", 64<<20, "Each table (or file) is at most this size.")
	numMemTables     = flag.Int("num-mem-tables", 3, "Maximum number of tables to keep in memory, before stalling.")
	numL0Table       = flag.Int("num-level-zero-tables", 3, "Maximum number of Level 0 tables before we start compacting.")
	syncWrites       = flag.Bool("sync-write", true, "Sync all writes to disk. Setting this to true would slow down data loading significantly.")
	maxProcs         = flag.Int("max-procs", 0, "Max CPU cores to use, set 0 to use all CPU cores in the machine.")
	shardKey         = flag.Bool("shard-key", false, "Enable shard key support. (need specified TiDB branch)")
)

var (
	gitHash = "None"
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

	numDB := 2
	if *shardKey {
		numDB = 8
		tikv.EnableSharding()
	}
	safePoint := &tikv.SafePoint{}
	dbs := make([]*badger.DB, numDB)
	for i := 0; i < numDB; i++ {
		dbs[i] = createDB(i, safePoint)
	}

	regionOpts := tikv.RegionOptions{
		StoreAddr:  *storeAddr,
		PDAddr:     *pdAddr,
		RegionSize: *regionSize,
	}
	rm := tikv.NewRegionManager(dbs, regionOpts)
	store := tikv.NewMVCCStore(dbs, *dbPath, safePoint)
	tikvServer := tikv.NewServer(rm, store)

	grpcServer := grpc.NewServer()
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

	err = rm.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("RegionManager closed.")

	for i, db := range dbs {
		err = db.Close()
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("DB%d closed.", i)
	}
}

func createDB(idx int, safePoint *tikv.SafePoint) *badger.DB {
	subPath := fmt.Sprintf("/%d", idx)
	opts := badger.DefaultOptions
	opts.ValueThreshold = *valThreshold
	opts.TableBuilderOptions.EnableHashIndex = false
	opts.TableBuilderOptions.BytesPerSync = 4 * 1024 * 1024
	opts.TableBuilderOptions.BytesPerSecond = 200 * 1024 * 1024
	opts.TableBuilderOptions.WriteBufferSize = 8 * 1024 * 1024
	opts.ValueLogWriteOptions.BytesPerSync = 0
	opts.ValueLogWriteOptions.WriteBufferSize = 8 * 1024 * 1024
	opts.Dir = *dbPath + subPath
	if *vlogPath != "" {
		opts.ValueDir = *vlogPath + subPath
	} else {
		opts.ValueDir = opts.Dir
	}
	if *tableLoadingMode == "memory-map" {
		opts.TableLoadingMode = options.MemoryMap
	}
	opts.ValueLogLoadingMode = options.FileIO
	opts.MaxTableSize = *maxTableSize
	opts.NumMemtables = *numMemTables
	opts.NumLevelZeroTables = *numL0Table
	opts.NumLevelZeroTablesStall = opts.NumLevelZeroTables + 5
	opts.SyncWrites = *syncWrites
	opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
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
