package raftstore

import (
	"context"
	"net"
	"os"
	"sync"

	"github.com/ngaut/log"
	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

type RaftInnerServer struct {
	engines       *Engines
	raftConfig    *Config
	storeMeta     metapb.Store
	eventObserver PeerEventObserver

	node            *Node
	snapManager     *SnapManager
	coprocessorHost *CoprocessorHost
	raftRouter      *RaftstoreRouter
	batchSystem     *raftBatchSystem
	pdWorker        *worker
	resolveWorker   *worker
	snapWorker      *worker
}

func (ris *RaftInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		ris.raftRouter.SendRaftMessage(msg)
	}
}

func (ris *RaftInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			ris.raftRouter.SendRaftMessage(msg)
		}
	}
}

func (ris *RaftInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	ris.snapWorker.scheduler <- task{
		tp: taskTypeSnapRecv,
		data: recvSnapTask{
			stream: stream,
			callback: func(e error) {
				err = e
				close(done)
			},
		},
	}
	<-done
	return err
}

func NewRaftInnerServer(engines *Engines, raftConfig *Config) *RaftInnerServer {
	return &RaftInnerServer{engines: engines, raftConfig: raftConfig}
}

func (ris *RaftInnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	ris.pdWorker = newWorker("pd-worker", &wg)
	ris.resolveWorker = newWorker("resolver", &wg)
	ris.snapWorker = newWorker("snap-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := ris.raftConfig
	router, batchSystem := createRaftBatchSystem(cfg)

	ris.raftRouter = NewRaftstoreRouter(router) // TODO: init with local reader
	ris.snapManager = NewSnapManager(cfg.SnapPath, router)
	ris.batchSystem = batchSystem
	ris.coprocessorHost = newCoprocessorHost(cfg.splitCheck, router)
}

func (ris *RaftInnerServer) GetRaftstoreRouter() *RaftstoreRouter {
	return ris.raftRouter
}

func (ris *RaftInnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *RaftInnerServer) SetPeerEventObserver(ob PeerEventObserver) {
	ris.eventObserver = ob
}

func (ris *RaftInnerServer) Start(pdClient pd.Client) error {
	ris.node = NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient, ris.eventObserver)

	raftClient := newRaftClient(ris.raftConfig)
	resolveSender := ris.resolveWorker.scheduler
	trans := NewServerTransport(raftClient, ris.snapWorker.scheduler, ris.raftRouter, resolveSender)

	resolveRunner := newResolverRunner(pdClient)
	ris.resolveWorker.start(resolveRunner)
	err := ris.node.Start(context.TODO(), ris.engines, trans, ris.snapManager, ris.pdWorker, ris.coprocessorHost, ris.raftRouter)
	if err != nil {
		return err
	}
	snapRunner := newSnapRunner(ris.snapManager, ris.raftConfig, ris.raftRouter)
	ris.snapWorker.start(snapRunner)
	return nil
}

func (ris *RaftInnerServer) Stop() error {
	ris.snapWorker.stop()
	ris.node.stop()
	ris.resolveWorker.stop()
	if err := ris.engines.raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.kv.db.Close(); err != nil {
		return err
	}
	return nil
}

type dummyEventObserver struct{}

func (*dummyEventObserver) OnPeerCreate(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerApplySnap(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerDestroy(ctx *PeerEventContext) {}

func (*dummyEventObserver) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*PeerEventContext) {
}

func (*dummyEventObserver) OnRegionConfChange(ctx *PeerEventContext, epoch *metapb.RegionEpoch) {}

func RunRaftServer(cfg *Config, pdClient pd.Client, engines *Engines, signalChan <-chan os.Signal) error {
	var wg sync.WaitGroup
	pdWorker := newWorker("pd-worker", &wg)
	resolveWorker := newWorker("resolver", &wg)
	resolveRunner := newResolverRunner(pdClient)
	resolveSender := resolveWorker.scheduler

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	router, batchSystem := createRaftBatchSystem(cfg)
	raftRouter := NewRaftstoreRouter(router) // TODO: init with local reader
	snapManager := NewSnapManager(cfg.SnapPath, router)
	var store metapb.Store
	node := NewNode(batchSystem, &store, cfg, pdClient, new(dummyEventObserver)) // TODO: Add PeerEventObserver

	// TODO: create storage

	server := NewServer(cfg, raftRouter, resolveSender, snapManager)

	coprocessorHost := newCoprocessorHost(cfg.splitCheck, router)

	resolveWorker.start(resolveRunner)

	err := node.Start(context.TODO(), engines, server.Trans(), snapManager, pdWorker, coprocessorHost, raftRouter)
	if err != nil {
		return err
	}

	err = server.Start()
	if err != nil {
		return err
	}

	<-signalChan

	// TODO: Be graceful!
	os.Exit(0)

	err = server.Stop()
	if err != nil {
		log.Errorf("failed to stop server: %v", err)
	}

	node.stop()

	resolveWorker.stop()

	wg.Wait()
	return nil
}

type Server struct {
	config      *Config
	wg          *sync.WaitGroup
	snapWorker  *worker
	grpcServer  *grpc.Server
	trans       *ServerTransport
	snapManager *SnapManager
	lis         net.Listener
}

func NewServer(config *Config, router *RaftstoreRouter, resovleSender chan<- task, snapManager *SnapManager) *Server {
	var wg sync.WaitGroup
	snapWorker := newWorker("snap-worker", &wg)
	kvService := NewKVService(router, snapWorker.scheduler)

	grpcOpts := []grpc.ServerOption{
		grpc.InitialConnWindowSize(2 * 1024 * 1024),
		grpc.MaxConcurrentStreams(1024),
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(1 << 32),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	tikvpb.RegisterTikvServer(grpcServer, kvService)

	raftClient := newRaftClient(config)
	trans := NewServerTransport(raftClient, snapWorker.scheduler, router, resovleSender)

	return &Server{
		config:      config,
		wg:          &wg,
		snapWorker:  snapWorker,
		grpcServer:  grpcServer,
		trans:       trans,
		snapManager: snapManager,
	}
}

func (s *Server) Start() error {
	snapRunner := newSnapRunner(s.snapManager, s.config, s.trans.raftRouter)
	s.snapWorker.start(snapRunner)
	lis, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	s.lis = lis
	go func() { s.grpcServer.Serve(lis) }()
	log.Info("tikv is ready to serve")
	return nil
}

func (s *Server) Trans() *ServerTransport {
	return s.trans
}

func (s *Server) Stop() error {
	s.snapWorker.stop()
	s.grpcServer.Stop()
	return s.lis.Close()
}
