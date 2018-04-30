package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/dgraph-io/badger"
	"github.com/ngaut/faketikv/tikv"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	pdAddr       = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
	storeAddr    = flag.String("store-addr", "127.0.0.1:9191", "store address")
	httpAddr     = flag.String("http-addr", "127.0.0.1:9291", "http address")
	dbPath       = flag.String("db-path", "/tmp/badger", "Directory to store the data in. Should exist and be writable.")
	valThreshold = flag.Int("value-threshold", 20, "If value size >= this threshold, only store value offsets in tree.")
	regionSize   = flag.Int64("region-size", 8*1024*1024, "Average region size.")
	logLevel     = flag.String("L", "info", "log level")
)

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)
	opts := badger.DefaultOptions
	opts.ValueThreshold = *valThreshold
	opts.Dir = *dbPath
	opts.ValueDir = *dbPath

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
	go http.ListenAndServe(*httpAddr, nil)
	err = grpcServer.Serve(l)
	log.Error(err)
}
