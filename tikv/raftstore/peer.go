package raftstore

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
	"math"
	"time"
)

type StaleState int

const (
	StaleStateValid StaleState = 0 + iota
	StaleStateToValidate
	StaleStateLeaderMissing
)

type ReqCbPair struct {
	Req *raft_cmdpb.RaftCmdRequest
	Cb  Callback
}

type ReadIndexRequest struct {
	id             uint64
	cmds           []*ReqCbPair
	renewLeaseTime *time.Time
}

func NewReadIndexRequest(id uint64, cmds []*ReqCbPair, renewLeaseTime *time.Time) *ReadIndexRequest {
	return &ReadIndexRequest{
		id:             id,
		cmds:           cmds,
		renewLeaseTime: renewLeaseTime,
	}
}

func (r *ReadIndexRequest) bianryId() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, r.id)
	return buf
}

type ReadIndexQueue struct {
	idAllocator uint64
	reads       []*ReadIndexRequest
	readyCnt    int
}

func (q *ReadIndexQueue) PopFront() *ReadIndexRequest {
	if len(q.reads) > 0 {
		req := q.reads[0]
		q.reads = q.reads[1:]
		return req
	}
	return nil
}

func NotifyStaleReq(term uint64, cb Callback) {
	resp := ErrResp(&ErrStaleCommand{}, term)
	cb(resp)
}

func NotifyReqRegionRemoved(regionId uint64, cb Callback) {
	regionNotFound := &ErrRegionNotFound{RegionId: regionId}
	resp := NewRespFromError(regionNotFound)
	cb(resp)
}

func (r *ReadIndexQueue) NextId() uint64 {
	r.idAllocator += 1
	return r.idAllocator
}

func (r *ReadIndexQueue) ClearUncommitted(term uint64) {
	uncommitted := r.reads[r.readyCnt:]
	r.reads = r.reads[:r.readyCnt]
	for _, read := range uncommitted {
		for _, reqCbPair := range read.cmds {
			NotifyStaleReq(term, reqCbPair.Cb)
		}
	}
}

type ProposalMeta struct {
	Index          uint64
	Term           uint64
	RenewLeaseTime *time.Time
}

type ProposalQueue struct {
	queue []*ProposalMeta
}

func (q *ProposalQueue) PopFront(term uint64) *ProposalMeta {
	if len(q.queue) == 0 || q.queue[0].Term > term {
		return nil
	}
	meta := q.queue[0]
	q.queue = q.queue[1:]
	return meta
}

func (q *ProposalQueue) Push(meta *ProposalMeta) {
	q.queue = append(q.queue, meta)
}

func (q *ProposalQueue) Clear() {
	q.queue = q.queue[:0]
}

type PeerStat struct {
	WrittenBytes uint64
	WrittenKeys  uint64
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
	// Todo: it is a place holder currently
}

/// A struct that stores the state to wait for `PrepareMerge` apply result.
///
/// When handling the apply result of a `CommitMerge`, the source peer may have
/// not handle the apply result of the `PrepareMerge`, so the target peer has
/// to abort current handle process and wait for it asynchronously.
type WaitApplyResultStat struct {
	/// The following apply results waiting to be handled, including the `CommitMerge`.
	/// These will be handled once `ReadyToMerge` is true.
	results []*ApplyTaskRes
	/// It is used by target peer to check whether the apply result of `PrepareMerge` is handled.
	readyToMerge *bool
}

type Proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	Cb           Callback
}

type RegionProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*Proposal
}

func NewRegionProposal(id uint64, regionId uint64, props []*Proposal) *RegionProposal {
	return &RegionProposal{
		Id:       id,
		RegionId: regionId,
		Props:    props,
	}
}

type RecentAddedPeer struct {
	RejectDurationAsSecs uint64
	Id                   uint64
	AddedTime            time.Time
}

func NewRecentAddedPeer(rejectDurationAsSecs uint64) *RecentAddedPeer {
	return &RecentAddedPeer{
		RejectDurationAsSecs: rejectDurationAsSecs,
		Id:                   0,
		AddedTime:            time.Now(),
	}
}

func (r *RecentAddedPeer) Update(id uint64, now time.Time) {
	r.Id = id
	r.AddedTime = now
}

func (r *RecentAddedPeer) Contains(id uint64) bool {
	if r.Id == id {
		now := time.Now()
		elapsedSecs := now.Sub(r.AddedTime).Seconds()
		return uint64(elapsedSecs) < r.RejectDurationAsSecs
	}
	return false
}

/// `ConsistencyState` is used for consistency check.
type ConsistencyState struct {
	LastCheckTime time.Time
	// (computed_result_or_to_be_verified, index, hash)
	Index uint64
	Hash  []byte
}

type DestroyPeerJob struct {
	Initialized bool
	AsyncRemove bool
	RegionId    uint64
	Peer        *metapb.Peer
}

type Peer struct {
	peerCache      map[uint64]*metapb.Peer
	Peer           *metapb.Peer
	regionId       uint64
	RaftGroup      *raft.RawNode
	peerStorage    *PeerStorage
	proposals      *ProposalQueue
	applyProposals []*Proposal
	pendingReads   *ReadIndexQueue

	// Record the last instant of each peer's heartbeat response.
	PeerHeartbeats map[uint64]time.Time

	/// Record the instants of peers being added into the configuration.
	/// Remove them after they are not pending any more.
	PeersStartPendingTime map[uint64]time.Time
	RecentAddedPeer       *RecentAddedPeer

	/// an inaccurate difference in region size since last reset.
	SizeDiffHint uint64
	/// delete keys' count since last reset.
	deleteKeysHint uint64
	/// approximate size of the region.
	ApproximateSize *uint64
	/// approximate keys of the region.
	ApproximateKeys         *uint64
	CompactionDeclinedBytes uint64

	ConsistencyState *ConsistencyState

	Tag string

	// Index of last scheduled committed raft log.
	LastApplyingIdx  uint64
	LastCompactedIdx uint64
	// The index of the latest urgent proposal index.
	lastUrgentProposalIdx uint64
	// The index of the latest committed split command.
	lastCommittedSplitIdx uint64
	// Approximate size of logs that is applied but not compacted yet.
	RaftLogSizeHint uint64

	PendingRemove bool

	// The index of the latest committed prepare merge command.
	lastCommittedPrepareMergeIdx uint64
	PendingMergeState            *rspb.MergeState
	leaderMissingTime            *time.Time
	leaderLease                  *Lease

	// If a snapshot is being applied asynchronously, messages should not be sent.
	pendingMessages         []eraftpb.Message
	PendingMergeApplyResult *WaitApplyResultStat
	PeerStat                *PeerStat
}

func NewPeer(storeId uint64, cfg *Config, engines *Engines, region *metapb.Region, peer *metapb.Peer) (*Peer, error) {
	if peer.GetId() == InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), peer.GetId())

	ps, err := NewPeerStorage(engines, region, tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:              peer.GetId(),
		ElectionTick:    cfg.RaftElectionTimeoutTicks,
		HeartbeatTick:   cfg.RaftHeartbeatTicks,
		MaxSizePerMsg:   cfg.RaftMaxSizePerMsg,
		MaxInflightMsgs: cfg.RaftMaxInflightMsgs,
		Applied:         appliedIndex,
		CheckQuorum:     true,
		PreVote:         cfg.Prevote,
		Storage:         ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg, nil)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	p := &Peer{
		Peer:                  peer,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		proposals:             new(ProposalQueue),
		pendingReads:          new(ReadIndexQueue),
		peerCache:             make(map[uint64]*metapb.Peer),
		PeerHeartbeats:        make(map[uint64]time.Time),
		PeersStartPendingTime: make(map[uint64]time.Time),
		RecentAddedPeer:       NewRecentAddedPeer(uint64(cfg.RaftRejectTransferLeaderDuration.Seconds())),
		ConsistencyState: &ConsistencyState{
			LastCheckTime: now,
			Index:         RaftInvalidIndex,
		},
		leaderMissingTime:     &now,
		Tag:                   tag,
		LastApplyingIdx:       appliedIndex,
		lastUrgentProposalIdx: math.MaxInt64,
		leaderLease:           NewLease(cfg.RaftStoreMaxLeaderLease),
		PeerStat:              new(PeerStat),
	}

	// If this region has only one peer and I am the one, campaign directly.
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}
