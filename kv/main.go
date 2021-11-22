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
	"github.com/pingcap-incubator/tinykv/kv/server"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	schedulerAddr = flag.String("scheduler", "", "scheduler address")
	storeAddr     = flag.String("addr", "", "store address")
	dbPath        = flag.String("path", "", "directory path of db")
	logLevel      = flag.String("loglevel", "", "the level of log")
)

func main() {
	flag.Parse()
	conf := config.NewDefaultConfig()
	if *schedulerAddr != "" {
		conf.SchedulerAddr = *schedulerAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	if *dbPath != "" {
		conf.DBPath = *dbPath
	}
	if *logLevel != "" {
		conf.LogLevel = *logLevel
	}

	// log level
	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("Server started with conf %+v", conf)

	var storage storage.Storage
	if conf.Raft {
		storage = raft_storage.NewRaftStorage(conf)
	} else {	// 单机版本
		storage = standalone_storage.NewStandAloneStorage(conf)
	}
	if err := storage.Start(); err != nil {
		log.Fatal(err)
	}
	server := server.NewServer(storage)

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(1<<30),
		grpc.InitialConnWindowSize(1<<30),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	// register grpcServer
	tinykvpb.RegisterTinyKvServer(grpcServer, server)
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr, ':'):]
	// create a listener
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	// 处理
	handleSignal(grpcServer)

	// 监听listener上的消息，为每个连接创建一个新的服务器，处理该gRPC请求
	// 每次的处理程序
	// 出现panic关闭listener
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
