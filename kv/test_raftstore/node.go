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

	sendFilters map[uint64][]Filter
	recvFilters map[uint64][]Filter
	routers     map[uint64]message.RaftRouter
	snapMgrs    map[uint64]*snap.SnapManager
}

func NewMockTransport() MockTransport {
	return MockTransport{
		sendFilters: make(map[uint64][]Filter),
		recvFilters: make(map[uint64][]Filter),
		routers:     make(map[uint64]message.RaftRouter),
		snapMgrs:    make(map[uint64]*snap.SnapManager),
	}
}

func (t *MockTransport) AddNode(nodeID uint64, raftRouter message.RaftRouter, snapMgr *snap.SnapManager) {
	t.Lock()
	defer t.Unlock()

	t.routers[nodeID] = raftRouter
	t.snapMgrs[nodeID] = snapMgr
}

func (t *MockTransport) getSendFilters(nodeID uint64) []Filter {
	filters := t.sendFilters[nodeID]
	if filters == nil {
		filters = make([]Filter, 0)
	}
	return filters
}

func (t *MockTransport) getRecvFilters(nodeID uint64) []Filter {
	filters := t.recvFilters[nodeID]
	if filters == nil {
		filters = make([]Filter, 0)
	}
	return filters
}

func (t *MockTransport) AddSendFilter(nodeID uint64, filter Filter) {
	t.Lock()
	defer t.Unlock()

	filters := t.getSendFilters(nodeID)
	filters = append(filters, filter)
	t.sendFilters[nodeID] = filters
}

func (t *MockTransport) AddReceiveFilter(nodeID uint64, filter Filter) {
	t.Lock()
	defer t.Unlock()

	filters := t.getRecvFilters(nodeID)
	filters = append(filters, filter)
	t.recvFilters[nodeID] = filters
}

func (t *MockTransport) ClearSendFilters(nodeID uint64) {
	t.Lock()
	defer t.Unlock()

	t.sendFilters[nodeID] = nil
}

func (t *MockTransport) ClearReceiveFilters(nodeID uint64) {
	t.Lock()
	defer t.Unlock()

	t.recvFilters[nodeID] = nil
}

func (t *MockTransport) Send(msg *raft_serverpb.RaftMessage) error {
	t.Lock()
	defer t.Unlock()

	fromStore := msg.GetFromPeer().GetStoreId()
	toStore := msg.GetToPeer().GetStoreId()

	fromFilters := t.getSendFilters(fromStore)
	toFilters := t.getRecvFilters(toStore)

	for _, filter := range fromFilters {
		if !filter.Before(msg) {
			return nil
		}
	}
	for _, filter := range toFilters {
		if !filter.Before(msg) {
			return nil
		}
	}

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

	for _, filter := range fromFilters {
		filter.After()
	}
	for _, filter := range toFilters {
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

func (c *NodeSimulator) AddSendFilter(nodeID uint64, filter Filter) {
	c.trans.AddSendFilter(nodeID, filter)
}

func (c *NodeSimulator) ClearSendFilters(nodeID uint64) {
	c.trans.ClearSendFilters(nodeID)
}

func (c *NodeSimulator) AddReceiveFilter(nodeID uint64, filter Filter) {
	c.trans.AddReceiveFilter(nodeID, filter)
}

func (c *NodeSimulator) ClearReceiveFilters(nodeID uint64) {
	c.trans.ClearReceiveFilters(nodeID)
}

func (c *NodeSimulator) GetNodeIds() []uint64 {
	nodeIDs := make([]uint64, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs
}

func (c *NodeSimulator) CallCommandOnNode(nodeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	c.trans.Lock()
	defer c.trans.Unlock()

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
