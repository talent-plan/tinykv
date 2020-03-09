package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type MsgApplyProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*proposal
}

type MsgApplyCommitted struct {
	regionId uint64
	term     uint64
	entries  []eraftpb.Entry
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	cb           *message.Callback
}

type MsgApplyRefresh struct {
	id     uint64
	term   uint64
	region *metapb.Region
}

type MsgApplyRes struct {
	regionID     uint64
	execResults  []execResult
	sizeDiffHint uint64
}

type execResult = interface{}

type execResultChangePeer struct {
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
	region     *metapb.Region
}

type execResultCompactLog struct {
	truncatedIndex uint64
	firstIndex     uint64
}

type execResultSplitRegion struct {
	regions []*metapb.Region
	derived *metapb.Region
}

/// The applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply worker.Task to make the code logic more clear.
type applier struct {
	// Your Data Here (2B).
}

func newApplierFromPeer(peer *peer) *applier {
	// Your Code Here (2B).
	return nil
}

func (a *applier) destroy() {
	// Your Code Here (3B).
}

func (a *applier) handleTask(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeApplyProposal:
		// Your Code Here (2B).
	case message.MsgTypeApplyCommitted:
		// Your Code Here (2B).
	case message.MsgTypeApplyRefresh:
		// Your Code Here (2C).
	}
}
