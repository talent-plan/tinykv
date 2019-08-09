package raftstore

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

/// router route messages to its target mailbox.
///
/// Every fsm has a mailbox, hence it's necessary to have an address book
/// that can deliver messages to specified fsm, which is exact router.
///
/// In our abstract model, every batch system has two different kind of
/// fsms. First is normal fsm, which does the common work like peers in a
/// raftstore model or apply delegate in apply model. Second is control fsm,
/// which does some work that requires a global view of resources or creates
/// missing fsm for specified address. Normal fsm and control fsm can have
/// different scheduler, but this is not required.
type router struct {
	pr *peerRouter
}

func newRouter(pr *peerRouter) *router {
	return &router{pr: pr}
}

func (r *router) register(addr uint64, peer *peerFsm) {
	r.pr.register(peer)
	return
}

var errMailboxNotFound = errors.New("mailbox not found")

func (r *router) send(addr uint64, msg Msg) error {
	msg.RegionID = addr
	return r.pr.send(addr, msg)
}

func (r *router) sendControl(msg Msg) {
	r.pr.sendStore(msg)
}

func (r *router) close(addr uint64) {
	r.pr.close(addr)
	log.Infof("region %d shutdown", addr)
}
