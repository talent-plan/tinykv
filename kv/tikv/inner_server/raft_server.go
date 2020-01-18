package inner_server

import (
	"context"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

type RaftInnerServer struct {
	engines       *engine_util.Engines
	raftConfig    *config.Config
	storeMeta     metapb.Store
	eventObserver raftstore.PeerEventObserver

	node          *raftstore.Node
	snapManager   *raftstore.SnapManager
	raftRouter    *raftstore.RaftstoreRouter
	batchSystem   *raftstore.RaftBatchSystem
	pdWorker      *worker.Worker
	resolveWorker *worker.Worker
	snapWorker    *worker.Worker
}

type TransportBuilder = func(snapScheduler chan<- worker.Task, raftRouter RaftRouter, resolverScheduler chan<- worker.Task) raftstore.Transport

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
	ris.snapWorker.Sender() <- worker.Task{
		Tp: worker.TaskTypeSnapRecv,
		Data: recvSnapTask{
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

func NewRaftInnerServer(engines *engine_util.Engines, raftConfig *config.Config) *RaftInnerServer {
	return &RaftInnerServer{engines: engines, raftConfig: raftConfig}
}

func (ris *RaftInnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	ris.pdWorker = worker.NewWorker("pd-worker", &wg)
	ris.resolveWorker = worker.NewWorker("resolver", &wg)
	ris.snapWorker = worker.NewWorker("snap-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := ris.raftConfig
	router, batchSystem := raftstore.CreateRaftBatchSystem(cfg)

	ris.raftRouter = raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	ris.snapManager = raftstore.NewSnapManager(cfg.SnapPath, router)
	ris.batchSystem = batchSystem
}

func (ris *RaftInnerServer) GetRaftstoreRouter() *raftstore.RaftstoreRouter {
	return ris.raftRouter
}

func (ris *RaftInnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *RaftInnerServer) SetPeerEventObserver(ob raftstore.PeerEventObserver) {
	ris.eventObserver = ob
}

func (ris *RaftInnerServer) Start(pdClient pd.Client, newTrans TransportBuilder) error {
	ris.node = raftstore.NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient, ris.eventObserver)

	resolveSender := ris.resolveWorker.Sender()
	trans := newTrans(ris.snapWorker.Sender(), ris.raftRouter, resolveSender)

	resolveRunner := newResolverRunner(pdClient)
	ris.resolveWorker.Start(resolveRunner)
	err := ris.node.Start(context.TODO(), ris.engines, trans, ris.snapManager, ris.pdWorker, ris.raftRouter)
	if err != nil {
		return err
	}
	snapRunner := newSnapRunner(ris.snapManager, ris.raftConfig, ris.raftRouter)
	ris.snapWorker.Start(snapRunner)
	return nil
}

func (ris *RaftInnerServer) Stop() error {
	ris.snapWorker.Stop()
	ris.node.Stop()
	ris.resolveWorker.Stop()
	if err := ris.engines.Raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}
