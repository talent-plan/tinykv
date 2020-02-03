package test_raftstore

import (
	"context"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type MockTransport struct {
	sync.Mutex
	routers map[uint64]message.RaftRouter
}

func NewMockTransport() MockTransport {
	return MockTransport{
		routers: make(map[uint64]message.RaftRouter),
	}
}

func (t *MockTransport) AddNode(storeID uint64, raftRouter message.RaftRouter) {
	t.Lock()
	defer t.Unlock()

	t.routers[storeID] = raftRouter
}

func (t *MockTransport) Send(msg *raft_serverpb.RaftMessage) error {
	t.Lock()
	defer t.Unlock()

	storeID := msg.GetToPeer().GetStoreId()
	router, ok := t.routers[storeID]
	if !ok {
		panic("storeID not found")
	}
	router.SendRaftMessage(msg)

	return nil
}

type NodeCluster struct {
	trans    *MockTransport
	pdClient pd.Client
	nodes    []*raftstore.Node
}

func NewNodeCluster(pdClient pd.Client) NodeCluster {
	trans := NewMockTransport()
	return NodeCluster{
		trans:    &trans,
		pdClient: pdClient,
	}
}

func (c *NodeCluster) RunNode(raftConf *tikvConf.Config, engine *engine_util.Engines, ctx context.Context) error {
	var wg sync.WaitGroup
	pdWorker := worker.NewWorker("pd-worker", &wg)

	router, batchSystem := raftstore.CreateRaftBatchSystem(raftConf)
	raftRouter := raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	snapManager := snap.NewSnapManager(raftConf.SnapPath)
	node := raftstore.NewNode(batchSystem, &metapb.Store{}, raftConf, c.pdClient)

	err := node.Start(ctx, engine, c.trans, snapManager, pdWorker, raftRouter)
	if err != nil {
		return err
	}
	c.nodes = append(c.nodes, node)

	c.trans.AddNode(node.GetStoreID(), raftRouter)

	return nil
}

func (c *NodeCluster) StopNodes() {
	for _, node := range c.nodes {
		node.Stop()
	}
}
