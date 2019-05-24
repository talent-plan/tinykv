package raftstore

import (
	"time"

	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
)

type RaftstoreRouter struct {
	router *router
	// TODO: add localReader here.
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) {
	r.router.sendRaftMessage(msg)
}

func (r *RaftstoreRouter) SendCommand(req *raft_cmdpb.RaftCmdRequest, cb *Callback) error {
	// TODO: support local reader
	msg := &MsgRaftCmd{
		SendTime: time.Now(),
		Request:  req,
		Callback: cb,
	}
	return r.router.sendRaftCommand(msg)
}

func (r *RaftstoreRouter) SignificantSend(regionID uint64, msg Msg) error {
	// TODO: no capacity check now, so no difference between send and SignificantSend.
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) ReportUnreachable(regionID, toPeerID uint64) error {
	return r.SignificantSend(regionID, NewMsg(MsgTypeSignificantMsg, &MsgSignificant{
		Type:     MsgSignificantTypeUnreachable,
		ToPeerID: toPeerID,
	}))
}

func (r *RaftstoreRouter) ReportSnapshotStatus(regionID uint64, toPeerID uint64, status raft.SnapshotStatus) error {
	return r.SignificantSend(regionID, NewMsg(MsgTypeSignificantMsg, &MsgSignificant{
		Type:           MsgSignificantTypeStatus,
		ToPeerID:       toPeerID,
		SnapshotStatus: status,
	}))
}
