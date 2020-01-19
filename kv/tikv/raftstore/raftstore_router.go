package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type MsgSignificantType int

const (
	MsgSignificantTypeStatus      MsgSignificantType = 1
	MsgSignificantTypeUnreachable MsgSignificantType = 2
)

type MsgSignificant struct {
	Type           MsgSignificantType
	ToPeerID       uint64
	SnapshotStatus raft.SnapshotStatus
}

type RaftstoreRouter struct {
	router *router
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) {
	r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) {
	r.router.sendRaftMessage(msg)
}

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	msg := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	return r.router.sendRaftCommand(msg)
}

func (r *RaftstoreRouter) SignificantSend(regionID uint64, msg message.Msg) error {
	// TODO: no capacity check now, so no difference between send and SignificantSend.
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) ReportUnreachable(regionID, toPeerID uint64) error {
	return r.SignificantSend(regionID, message.NewMsg(message.MsgTypeSignificantMsg, &MsgSignificant{
		Type:     MsgSignificantTypeUnreachable,
		ToPeerID: toPeerID,
	}))
}

func (r *RaftstoreRouter) ReportSnapshotStatus(regionID uint64, toPeerID uint64, status raft.SnapshotStatus) error {
	return r.SignificantSend(regionID, message.NewMsg(message.MsgTypeSignificantMsg, &MsgSignificant{
		Type:           MsgSignificantTypeStatus,
		ToPeerID:       toPeerID,
		SnapshotStatus: status,
	}))
}
