package test_raftstore

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type MockTransport struct {
	sync.Mutex

	filters  []Filter
	routers  map[uint64]message.RaftRouter
	snapMgrs map[uint64]*snap.SnapManager
}

func NewMockTransport() MockTransport {
	return MockTransport{
		routers:  make(map[uint64]message.RaftRouter),
		snapMgrs: make(map[uint64]*snap.SnapManager),
	}
}

func (t *MockTransport) AddNode(nodeID uint64, raftRouter message.RaftRouter, snapMgr *snap.SnapManager) {
	t.Lock()
	defer t.Unlock()

	t.routers[nodeID] = raftRouter
	t.snapMgrs[nodeID] = snapMgr
}

func (t *MockTransport) AddFilter(filter Filter) {
	t.Lock()
	defer t.Unlock()

	t.filters = append(t.filters, filter)
}

func (t *MockTransport) ClearFilters() {
	t.Lock()
	defer t.Unlock()

	t.filters = nil
}

func (t *MockTransport) Send(msg *raft_serverpb.RaftMessage) error {
	t.Lock()
	defer t.Unlock()

	for _, filter := range t.filters {
		if !filter.Before(msg) {
			return nil
		}
	}

	fromStore := msg.GetFromPeer().GetStoreId()
	toStore := msg.GetToPeer().GetStoreId()

	regionID := msg.GetRegionId()
	toPeerID := msg.GetToPeer().GetId()
	isSnapshot := msg.GetMessage().GetMsgType() == eraftpb.MessageType_MsgSnapshot

	if isSnapshot {
		snapshot := msg.Message.Snapshot
		key, err := snap.SnapKeyFromSnap(snapshot)

		fromSnapMgr := t.snapMgrs[fromStore]
		if err != nil {
			return err
		}
		fromSnapMgr.Register(key, snap.SnapEntrySending)
		fromSnap, err := fromSnapMgr.GetSnapshotForSending(key)
		if err != nil {
			return err
		}

		toSnapMgr := t.snapMgrs[toStore]
		if err != nil {
			return err
		}
		toSnapMgr.Register(key, snap.SnapEntryReceiving)
		toSnap, err := toSnapMgr.GetSnapshotForReceiving(key, snapshot.GetData())
		if err != nil {
			return err
		}

		io.Copy(toSnap, fromSnap)

		toSnapMgr.Deregister(key, snap.SnapEntryReceiving)
		fromSnapMgr.Deregister(key, snap.SnapEntrySending)
	}

	router, _ := t.routers[toStore]
	router.SendRaftMessage(msg)
	if isSnapshot {
		err := router.ReportSnapshotStatus(regionID, toPeerID, raft.SnapshotFinish)
		if err != nil {
			return err
		}
	}

	for _, filter := range t.filters {
		filter.After()
	}

	return nil
}

type NodeSimulator struct {
	trans    *MockTransport
	pdClient pd.Client
	nodes    map[uint64]*raftstore.Node
}

func NewNodeSimulator(pdClient pd.Client) NodeSimulator {
	trans := NewMockTransport()
	return NodeSimulator{
		trans:    &trans,
		pdClient: pdClient,
		nodes:    make(map[uint64]*raftstore.Node),
	}
}

func (c *NodeSimulator) RunNode(raftConf *tikvConf.Config, engine *engine_util.Engines, ctx context.Context) error {
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

	storeID := node.GetStoreID()
	c.nodes[storeID] = node
	c.trans.AddNode(storeID, raftRouter, snapManager)

	return nil
}

func (c *NodeSimulator) StopNode(nodeID uint64) {
	node := c.nodes[nodeID]
	if node == nil {
		log.Panicf("Can not find node %d", nodeID)
	}
	node.Stop()
}

func (c *NodeSimulator) AddFilter(filter Filter) {
	c.trans.AddFilter(filter)
}

func (c *NodeSimulator) ClearFilters() {
	c.trans.ClearFilters()
}

func (c *NodeSimulator) GetNodeIds() []uint64 {
	nodeIDs := make([]uint64, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs
}

func (c *NodeSimulator) CallCommandOnNode(nodeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	router := c.trans.routers[nodeID]
	if router == nil {
		log.Panicf("Can not find node %d", nodeID)
	}

	cb := message.NewCallback()
	err := router.SendRaftCommand(request, cb)
	if err != nil {
		panic(err)
	}

	resp := cb.WaitRespWithTimeout(timeout)

	if resp == nil {
		return nil
	}

	reqCount := len(request.Requests)

	if resp.Header != nil && resp.Header.Error != nil {
		panic(&resp.Header.Error)
	}
	if len(resp.Responses) != reqCount {
		log.Panicf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}

	return resp
}
