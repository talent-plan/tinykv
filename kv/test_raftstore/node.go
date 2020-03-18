package test_raftstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type MockTransport struct {
	sync.RWMutex

	filters  []Filter
	routers  map[uint64]message.RaftRouter
	snapMgrs map[uint64]*snap.SnapManager
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		routers:  make(map[uint64]message.RaftRouter),
		snapMgrs: make(map[uint64]*snap.SnapManager),
	}
}

func (t *MockTransport) AddStore(storeID uint64, raftRouter message.RaftRouter, snapMgr *snap.SnapManager) {
	t.Lock()
	defer t.Unlock()

	t.routers[storeID] = raftRouter
	t.snapMgrs[storeID] = snapMgr
}

func (t *MockTransport) RemoveStore(storeID uint64) {
	t.Lock()
	defer t.Unlock()

	delete(t.routers, storeID)
	delete(t.snapMgrs, storeID)
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
	t.RLock()
	defer t.RUnlock()

	for _, filter := range t.filters {
		if !filter.Before(msg) {
			return errors.New(fmt.Sprintf("message %+v is dropped", msg))
		}
	}

	fromStore := msg.GetFromPeer().GetStoreId()
	toStore := msg.GetToPeer().GetStoreId()

	isSnapshot := msg.GetMessage().GetMsgType() == eraftpb.MessageType_MsgSnapshot
	if isSnapshot {
		snapshot := msg.Message.Snapshot
		key, err := snap.SnapKeyFromSnap(snapshot)
		if err != nil {
			return err
		}

		fromSnapMgr, found := t.snapMgrs[fromStore]
		if !found {
			return errors.New(fmt.Sprintf("store %d is closed", fromStore))
		}
		fromSnapMgr.Register(key, snap.SnapEntrySending)
		fromSnap, err := fromSnapMgr.GetSnapshotForSending(key)
		if err != nil {
			return err
		}

		toSnapMgr, found := t.snapMgrs[toStore]
		if !found {
			return errors.New(fmt.Sprintf("store %d is closed", toStore))
		}
		toSnapMgr.Register(key, snap.SnapEntryReceiving)
		toSnap, err := toSnapMgr.GetSnapshotForReceiving(key, snapshot.GetData())
		if err != nil {
			return err
		}

		io.Copy(toSnap, fromSnap)
		toSnap.Save()

		toSnapMgr.Deregister(key, snap.SnapEntryReceiving)
		fromSnapMgr.Deregister(key, snap.SnapEntrySending)
	}

	router, found := t.routers[toStore]
	if !found {
		return errors.New(fmt.Sprintf("store %d is closed", toStore))
	}
	router.SendRaftMessage(msg)

	for _, filter := range t.filters {
		filter.After()
	}

	return nil
}

type NodeSimulator struct {
	sync.RWMutex

	trans           *MockTransport
	schedulerClient scheduler_client.Client
	nodes           map[uint64]*raftstore.Node
}

func NewNodeSimulator(schedulerClient scheduler_client.Client) *NodeSimulator {
	trans := NewMockTransport()
	return &NodeSimulator{
		trans:           trans,
		schedulerClient: schedulerClient,
		nodes:           make(map[uint64]*raftstore.Node),
	}
}

func (c *NodeSimulator) RunStore(cfg *config.Config, engine *engine_util.Engines, ctx context.Context) error {
	c.Lock()
	defer c.Unlock()

	raftRouter, batchSystem := raftstore.CreateRaftBatchSystem(cfg)
	snapManager := snap.NewSnapManager(cfg.DBPath + "/snap")
	node := raftstore.NewNode(batchSystem, cfg, c.schedulerClient)

	err := node.Start(ctx, engine, c.trans, snapManager)
	if err != nil {
		return err
	}

	storeID := node.GetStoreID()
	c.nodes[storeID] = node
	c.trans.AddStore(storeID, raftRouter, snapManager)

	return nil
}

func (c *NodeSimulator) StopStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()

	node := c.nodes[storeID]
	if node == nil {
		panic(fmt.Sprintf("Can not find store %d", storeID))
	}
	node.Stop()
	delete(c.nodes, storeID)
	c.trans.RemoveStore(storeID)
}

func (c *NodeSimulator) AddFilter(filter Filter) {
	c.Lock()
	defer c.Unlock()
	c.trans.AddFilter(filter)
}

func (c *NodeSimulator) ClearFilters() {
	c.Lock()
	defer c.Unlock()
	c.trans.ClearFilters()
}

func (c *NodeSimulator) GetStoreIds() []uint64 {
	c.RLock()
	defer c.RUnlock()
	storeIDs := make([]uint64, 0, len(c.nodes))
	for storeID := range c.nodes {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

func (c *NodeSimulator) CallCommandOnStore(storeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	c.RLock()
	router := c.trans.routers[storeID]
	if router == nil {
		log.Fatalf("Can not find node %d", storeID)
	}
	c.RUnlock()

	cb := message.NewCallback()
	err := router.SendRaftCommand(request, cb)
	if err != nil {
		return nil, nil
	}

	resp := cb.WaitRespWithTimeout(timeout)
	return resp, cb.Txn
}
