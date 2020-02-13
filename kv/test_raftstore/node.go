package test_raftstore

import (
	"context"
	"io"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
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

func (t *MockTransport) AddNode(storeID uint64, raftRouter message.RaftRouter, snapMgr *snap.SnapManager) {
	t.Lock()
	defer t.Unlock()

	t.routers[storeID] = raftRouter
	t.snapMgrs[storeID] = snapMgr
}

func (t *MockTransport) getSendFilters(storeID uint64) []Filter {
	filters := t.sendFilters[storeID]
	if filters == nil {
		filters = make([]Filter, 0, 0)
	}
	return filters
}

func (t *MockTransport) getRecvFilters(storeID uint64) []Filter {
	filters := t.recvFilters[storeID]
	if filters == nil {
		filters = make([]Filter, 0, 0)
	}
	return filters
}

func (t *MockTransport) AddSendFilter(storeID uint64, filter Filter) {
	t.Lock()
	defer t.Unlock()

	filters := t.getSendFilters(storeID)
	filters = append(filters, filter)
	t.sendFilters[storeID] = filters
}

func (t *MockTransport) AddReceiveFilter(storeID uint64, filter Filter) {
	t.Lock()
	defer t.Unlock()

	filters := t.getRecvFilters(storeID)
	filters = append(filters, filter)
	t.recvFilters[storeID] = filters
}

func (t *MockTransport) ClearSendFilters(storeID uint64) {
	t.Lock()
	defer t.Unlock()

	t.sendFilters[storeID] = nil
}

func (t *MockTransport) ClearReceiveFilters(storeID uint64) {
	t.Lock()
	defer t.Unlock()

	t.recvFilters[storeID] = nil
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

	c.trans.AddNode(node.GetStoreID(), raftRouter, snapManager)

	return nil
}

func (c *NodeCluster) StopNodes() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

func (c *NodeCluster) AddSendFilter(storeID uint64, filter Filter) {
	c.trans.AddSendFilter(storeID, filter)
}

func (c *NodeCluster) ClearSendFilters(storeID uint64) {
	c.trans.ClearSendFilters(storeID)
}

func (c *NodeCluster) AddReceiveFilter(storeID uint64, filter Filter) {
	c.trans.AddReceiveFilter(storeID, filter)
}

func (c *NodeCluster) ClearReceiveFilters(storeID uint64) {
	c.trans.ClearReceiveFilters(storeID)
}
