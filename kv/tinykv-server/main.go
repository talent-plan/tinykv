package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
)

func main() {
	flag.Parse()
	conf := loadConfig()
	if *pdAddr != "" {
		conf.Server.PDAddr = *pdAddr
	}
	if *storeAddr != "" {
		conf.Server.StoreAddr = *storeAddr
	}
	runtime.GOMAXPROCS(conf.Server.MaxProcs)
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(conf.Server.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("conf %v", conf)
	config.SetGlobalConf(conf)

	pdClient, err := pd.NewClient(strings.Split(conf.Server.PDAddr, ","), "")
	if err != nil {
		log.Fatal(err)
	}

	var innerServer tikv.InnerServer
	if conf.Server.Raft {
		innerServer = setupRaftInnerServer(pdClient, conf)
	} else {
		innerServer = setupStandAloneInnerServer(pdClient, conf)
	}
	tikvServer := tikv.NewServer(innerServer)

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
	listenAddr := conf.Server.StoreAddr[strings.IndexByte(conf.Server.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)
	go func() {
		log.Infof("listening on %v", conf.Server.StatusAddr)
		http.HandleFunc("/status", func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
		err := http.ListenAndServe(conf.Server.StatusAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal(err)
	}
	err = tikvServer.Stop()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Server stopped.")
}

func loadConfig() *config.Config {
	conf := config.DefaultConf
	if *configPath != "" {
		_, err := toml.DecodeFile(*configPath, &conf)
		if err != nil {
			panic(err)
		}
	}
	y.Assert(len(conf.Engine.Compression) >= badger.DefaultOptions.TableBuilderOptions.MaxLevels)
	return &conf
}

func setupRaftInnerServer(pdClient pd.Client, conf *config.Config) tikv.InnerServer {
	innerServer := inner_server.NewRaftInnerServer(conf)
	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer
}

func setupStandAloneInnerServer(pdClient pd.Client, conf *config.Config) tikv.InnerServer {
	innerServer := inner_server.NewStandAloneInnerServer(conf)
	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer
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
