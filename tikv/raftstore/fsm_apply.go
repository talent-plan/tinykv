package raftstore

import (
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type changePeer struct {
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
	region     *metapb.Region
}

type keyRange struct {
	startKey []byte
	endKey   []byte
}

type ApplyTask struct {
	RegionId uint64
	Term     uint64
	Entries  []eraftpb.Entry
}

type ApplyMetrics struct {
	SizeDiffHint       uint64
	DeleteKeysHint     uint64
	WrittenBytes       uint64
	WrittenKeys        uint64
	LockCfWrittenBytes uint64
}

type ApplyTaskRes struct {
	regionID         uint64
	applyState       rspb.RaftApplyState
	appliedIndexTerm uint64
	execResults      []execResult
	metrics          *ApplyMetrics
	merged           bool

	destroyPeerID uint64
}

type execResultChangePeer struct {
	cp changePeer
}

type execResultCompactLog struct {
	state      *rspb.RaftTruncatedState
	firstIndex uint64
}

type execResultSplitRegion struct {
	regions []*metapb.Region
	derived *metapb.Region
}

type execResultPrepareMerge struct {
	region *metapb.Region
	state  *rspb.MergeState
}

type execResultCommitMerge struct {
	region *metapb.Region
	source *metapb.Region
}

type execResultRollbackMerge struct {
	region *metapb.Region
	commit uint64
}

type execResultComputeHash struct {
	region *metapb.Region
	index  uint64
	snap   *DBSnapshot
}

type execResultVerifyHash struct {
	index uint64
	hash  []byte
}

type execResultDeleteRange struct {
	ranges []keyRange
}

type execResult struct {
	data interface{}
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	Cb           Callback
}

type regionProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*proposal
}

func newRegionProposal(id uint64, regionId uint64, props []*proposal) *regionProposal {
	return &regionProposal{
		Id:       id,
		RegionId: regionId,
		Props:    props,
	}
}

type registration struct {
	id               uint64
	term             uint64
	applyState       *rspb.RaftApplyState
	appliedIndexTerm uint64
	region           *metapb.Region
}

type applyRouter struct {
	router
}

func (r *applyRouter) scheduleTask(regionId uint64, msg Msg) {
	// TODO: stub
}

type notifier struct {
	router *router
}

type applyPollerBuilder struct {
}

func newApplyPollerBuilder(raftPollerBuilder *raftPollerBuilder, sender notifier, router *applyRouter) *applyPollerBuilder {
	return nil // TODO: stub
}

func (b *applyPollerBuilder) build() pollHandler {
	return nil // TODO: stub
}

type applyFsm struct {
}

func newApplyFsmFromPeer(peer *peerFsm) (chan<- Msg, fsm) {
	return nil, nil // TODO: stub
}
