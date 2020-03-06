package main

import (
	"flag"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/server"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr    = flag.String("pd", "", "pd address")
	storeAddr = flag.String("addr", "", "store address")
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

func main() {
	flag.Parse()
	conf := config.NewDefaultConfig()
	if *pdAddr != "" {
		conf.PDAddr = *pdAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("conf %v", conf)

	pdClient, err := pd.NewClient(strings.Split(conf.PDAddr, ","), "")
	if err != nil {
		log.Fatal(err)
	}

	var innerServer inner_server.InnerServer
	if conf.Raft {
		innerServer = inner_server.NewRaftInnerServer(conf)
	} else {
		innerServer = inner_server.NewStandAloneInnerServer(conf)
	}
	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}
	tikvServer := server.NewServer(innerServer)

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
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)

	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Server stopped.")
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
