package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type RaftstoreRouter struct {
	router *router
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) error {
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	return r.router.sendRaftMessage(msg)
}

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	msg := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	return r.router.sendRaftCommand(msg)
}
