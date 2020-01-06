package raftstore

import (
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type RaftRouter interface {
	SendRaftMessage(msg *raft_serverpb.RaftMessage)
	ReportSnapshotStatus(regionID uint64, toPeerID uint64, status raft.SnapshotStatus) error
	ReportUnreachable(regionID, toPeerID uint64) error
}

type ServerTransport struct {
	raftClient        *RaftClient
	raftRouter        RaftRouter
	resolverScheduler chan<- task
	snapScheduler     chan<- task
	resolving         sync.Map
}

func NewServerTransport(raftClient *RaftClient, snapScheduler chan<- task, raftRouter RaftRouter, resolverScheduler chan<- task) *ServerTransport {
	return &ServerTransport{
		raftClient:        raftClient,
		raftRouter:        raftRouter,
		resolverScheduler: resolverScheduler,
		snapScheduler:     snapScheduler,
	}
}

func (t *ServerTransport) Send(msg *raft_serverpb.RaftMessage) error {
	storeID := msg.GetToPeer().GetStoreId()
	t.SendStore(storeID, msg)
	return nil
}

func (t *ServerTransport) SendStore(storeID uint64, msg *raft_serverpb.RaftMessage) {
	addr := t.raftClient.GetAddr(storeID)
	if addr != "" {
		t.WriteData(storeID, addr, msg)
		return
	}
	if _, ok := t.resolving.Load(storeID); ok {
		log.Debugf("store address is being resolved, msg dropped. storeID: %v, msg: %s", storeID, msg)
		t.ReportUnreachable(msg)
		return
	}
	log.Debug("begin to resolve store address. storeID: %v", storeID)
	t.resolving.Store(storeID, struct{}{})
	t.Resolve(storeID, msg)
}

func (t *ServerTransport) Resolve(storeID uint64, msg *raft_serverpb.RaftMessage) {
	callback := func(addr string, err error) {
		// clear resolving
		t.resolving.Delete(storeID)
		if err != nil {
			log.Errorf("resolve store address failed. storeID: %v, err: %v", storeID, err)
			t.ReportUnreachable(msg)
			return
		}
		t.raftClient.InsertAddr(storeID, addr)
		t.WriteData(storeID, addr, msg)
		t.raftClient.Flush()
	}
	t.resolverScheduler <- task{
		tp: taskTypeResolveAddr,
		data: resolveAddrTask{
			storeID:  storeID,
			callback: callback,
		},
	}
}

func (t *ServerTransport) WriteData(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) {
	if msg.GetMessage().GetSnapshot() != nil {
		t.SendSnapshotSock(addr, msg)
		return
	}
	if err := t.raftClient.Send(storeID, addr, msg); err != nil {
		log.Errorf("send raft msg err. err: %v", err)
	}
}

func (t *ServerTransport) SendSnapshotSock(addr string, msg *raft_serverpb.RaftMessage) {
	callback := func(err error) {
		if err != nil {
			t.ReportSnapshotStatus(msg, raft.SnapshotFailure)
		} else {
			t.ReportSnapshotStatus(msg, raft.SnapshotFinish)
		}
	}

	task := task{
		tp: taskTypeSnapSend,
		data: sendSnapTask{
			addr:     addr,
			msg:      msg,
			callback: callback,
		},
	}
	t.snapScheduler <- task
}

func (t *ServerTransport) ReportSnapshotStatus(msg *raft_serverpb.RaftMessage, status raft.SnapshotStatus) {
	regionID := msg.GetRegionId()
	toPeerID := msg.GetToPeer().GetId()
	toStoreID := msg.GetToPeer().GetStoreId()
	log.Debugf("send snapshot. toPeerID: %v, regionID: %v, status: %v", toPeerID, regionID, status)
	if err := t.raftRouter.ReportSnapshotStatus(regionID, toPeerID, status); err != nil {
		log.Errorf("report snapshot to peer fails. toPeerID: %v, toStoreID: %v, regionID: %v, err: %v", toPeerID, toStoreID, regionID, err)
	}
}

func (t *ServerTransport) ReportUnreachable(msg *raft_serverpb.RaftMessage) {
	regionID := msg.GetRegionId()
	toPeerID := msg.GetToPeer().GetId()
	toStoreID := msg.GetToPeer().GetStoreId()
	if msg.GetMessage().GetMsgType() == eraftpb.MessageType_MsgSnapshot {
		t.ReportSnapshotStatus(msg, raft.SnapshotFailure)
		return
	}
	if err := t.raftRouter.ReportUnreachable(regionID, toPeerID); err != nil {
		log.Errorf("report peer unreachable failed. regionID: %v, toStoreID: %v, toPeerID: %v, err: %v", regionID, toStoreID, toPeerID, err)
	}
}

func (t *ServerTransport) Flush() {
	t.raftClient.Flush()
}
