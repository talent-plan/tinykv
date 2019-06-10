package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/raftstore"
)

var (
	pdAddr    = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
	storeAddr = flag.String("store-addr", "127.0.0.1:20160", "store address")
	dataPath  = flag.String("data", "/tmp/unikv", "data directory")

	valThreshold     = flag.Int("value-threshold", 20, "If value size >= this threshold, only store value offsets in tree.")
	tableLoadingMode = flag.String("table-loading-mode", "memory-map", "How should LSM tree be accessed. (memory-map/load-to-ram)")
	maxTableSize     = flag.Int64("max-table-size", 64<<20, "Each table (or file) is at most this size.")
	numMemTables     = flag.Int("num-mem-tables", 3, "Maximum number of tables to keep in memory, before stalling.")
	numL0Table       = flag.Int("num-level-zero-tables", 3, "Maximum number of Level 0 tables before we start compacting.")
	syncWrites       = flag.Bool("sync-write", true, "Sync all writes to disk. Setting this to true would slow down data loading significantly.")
)

func main() {
	flag.Parse()

	kvPath := filepath.Join(*dataPath, "kv")
	raftPath := filepath.Join(*dataPath, "raft")
	snapPath := filepath.Join(*dataPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	config := raftstore.NewDefaultConfig()
	config.Addr = *storeAddr
	config.SnapPath = snapPath

	pdClient, err := pd.NewClient(*pdAddr, "unikv")
	if err != nil {
		log.Fatal(err)
	}

	var safePoint tikv.SafePoint
	kvDB := createDB(kvPath, &safePoint)
	raftDB := createDB(raftPath, &safePoint)

	engines := raftstore.NewEngines(kvDB, raftDB, kvPath, raftPath)

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	log.Fatal(raftstore.RunRaftServer(config, pdClient, engines, signalChan))
}

func createDB(path string, safePoint *tikv.SafePoint) *badger.DB {
	opts := badger.DefaultOptions
	opts.ValueThreshold = *valThreshold
	opts.TableBuilderOptions.EnableHashIndex = false
	opts.TableBuilderOptions.BytesPerSync = 4 * 1024 * 1024
	opts.TableBuilderOptions.BytesPerSecond = 200 * 1024 * 1024
	opts.TableBuilderOptions.WriteBufferSize = 8 * 1024 * 1024
	opts.ValueLogWriteOptions.BytesPerSync = 0
	opts.ValueLogWriteOptions.WriteBufferSize = 8 * 1024 * 1024
	opts.Dir = path
	opts.ValueDir = path
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
