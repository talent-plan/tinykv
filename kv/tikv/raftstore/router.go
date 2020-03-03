package raftstore

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// router routes a message to a peer.
type router struct {
	peers       sync.Map
	peerSender  chan message.Msg
	storeSender chan<- message.Msg
}

func newRouter(storeSender chan<- message.Msg) *router {
	pm := &router{
		peerSender:  make(chan message.Msg, 40960),
		storeSender: storeSender,
	}
	return pm
}

func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *router) register(peer *peerFsm) {
	id := peer.peer.regionId
	newPeer := &peerState{
		peer:  peer,
		apply: newApplierFromPeer(peer.peer),
	}
	pr.peers.Store(id, newPeer)
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		ps.apply.destroy()
		pr.peers.Delete(regionID)
	}
}

func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		return errPeerNotFound
	}
	pr.peerSender <- msg
	return nil
}

func (pr *router) sendRaftCommand(cmd *message.MsgRaftCmd) error {
	regionID := cmd.Request.Header.RegionId
	return pr.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}

func (pr *router) sendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if pr.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		pr.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil
}

func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

var errPeerNotFound = errors.New("peer not found")
