package raftstore

import (
	"encoding/binary"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
	"math"
	"time"
)

type RegionChangeEvent int

const (
	RegionChangeEvent_Create RegionChangeEvent = 0 + iota
	RegionChangeEvent_Update
)

type CoprocessorHost struct {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) PrePropose(region *metapb.Region, req *raft_cmdpb.RaftCmdRequest) error {
	// Todo: currently it is a place holder
	return nil
}

func (c *CoprocessorHost) OnRegionChanged(region *metapb.Region, event RegionChangeEvent, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) OnRoleChanged(region *metapb.Region, role raft.StateType) {
	// Todo: currently it is a place holder
}

type ApplyRouter struct {
	// Todo: currently it is a place holder
}

func (a *ApplyRouter) ScheduleTask(regionId uint64, task *ApplyTask) {
	// Todo: currently it is a place holder
}

type ReadyICPair struct {
	Ready raft.Ready
	IC    *InvokeContext
}

type PollContext struct {
	Cfg             *Config
	CoprocessorHost *CoprocessorHost
	engine          *Engines
	dbBundle        *DBBundle
	applyRouter     *ApplyRouter
	needFlushTrans  bool
	trans           chan<- *rspb.RaftMessage
	ReadyRes        []ReadyICPair
	kvWB            *WriteBatch
	raftWB          *WriteBatch
	syncLog         bool
}

func (pc *PollContext) KVWB() *WriteBatch {
	return pc.kvWB
}

func (pc *PollContext) RaftWB() *WriteBatch {
	return pc.raftWB
}

func (pc *PollContext) SyncLog() bool {
	return pc.syncLog
}

func (pc *PollContext) SetSyncLog(sync bool) {
	pc.syncLog = sync
}

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
	cb(resp, nil)
}

func NotifyReqRegionRemoved(regionId uint64, cb Callback) {
	regionNotFound := &ErrRegionNotFound{RegionId: regionId}
	resp := NewRespFromError(regionNotFound)
	cb(resp, nil)
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

/// Register self to applyRouter so that the peer is then usable.
/// Also trigger `RegionChangeEvent::Create` here.
func (p *Peer) Activate(ctx *PollContext) {
	ctx.applyRouter.ScheduleTask(p.regionId, &ApplyTask{ /*todo: register self*/ })
	ctx.CoprocessorHost.OnRegionChanged(p.Region(), RegionChangeEvent_Create, p.GetRole())
}

func (p *Peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
func (p *Peer) MaybeDestroy() *DestroyPeerJob {
	if p.PendingRemove {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return nil
	}
	initialized := p.peerStorage.isInitialized()
	asyncRemove := false
	if p.IsApplyingSnapshot() {
		if !p.Store().CancelApplyingSnap() {
			log.Infof("%v stale peer %v is applying snapshot")
			return nil
		}
		// There is no tasks in apply/local read worker.
		asyncRemove = false
	} else {
		asyncRemove = initialized
	}
	p.PendingRemove = true

	return &DestroyPeerJob{
		AsyncRemove: asyncRemove,
		Initialized: initialized,
		RegionId:    p.regionId,
		Peer:        ClonePeer(p.Peer),
	}
}

/// Does the real destroy task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *Peer) Destroy(ctx *PollContext, keepData bool) error {
	start := time.Now()
	region := CloneRegion(p.Region())
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(WriteBatch)
	raftWB := new(WriteBatch)
	if err := p.Store().clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	var mergeState *rspb.MergeState
	if p.PendingMergeState != nil {
		mergeState = CloneMergeState(p.PendingMergeState)
	}
	if err := WritePeerState(kvWB, region, rspb.PeerState_Tombstone, mergeState); err != nil {
		return err
	}
	// write kv rocksdb first in case of restart happen between two write
	// Todo: sync = ctx.cfg.sync_log
	if err := kvWB.WriteToDB(ctx.engine.kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(ctx.engine.raft); err != nil {
		return err
	}

	if p.Store().isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		if err := p.Store().ClearData(); err != nil {
			log.Errorf("%v failed to schedule clear data task %v", p.Tag, err)
		}
	}

	for _, read := range p.pendingReads.reads {
		for _, r := range read.cmds {
			NotifyReqRegionRemoved(region.Id, r.Cb)
		}
		read.cmds = nil
	}
	p.pendingReads.reads = nil

	for _, proposal := range p.applyProposals {
		NotifyReqRegionRemoved(region.Id, proposal.Cb)
	}
	p.applyProposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *Peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *Peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// Set the region of a peer.
///
/// This will update the region of the peer, caller must ensure the region
/// has been preserved in a durable device.
func (p *Peer) SetRegion(host *CoprocessorHost, region *metapb.Region) {
	if p.Region().GetRegionEpoch().GetVersion() < region.GetRegionEpoch().GetVersion() {
		// Epoch version changed, disable read on the localreader for this region.
		p.leaderLease.ExpireRemoteLease()
	}
	p.Store().SetRegion(CloneRegion(region))

	if !p.PendingRemove {
		host.OnRegionChanged(p.Region(), RegionChangeEvent_Update, p.GetRole())
	}
}

func (p *Peer) PeerId() uint64 {
	return p.Peer.GetId()
}

func (p *Peer) GetRaftStatus() *raft.Status {
	return p.RaftGroup.Status()
}

func (p *Peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *Peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

func (p *Peer) GetRole() raft.StateType {
	return p.RaftGroup.Raft.State
}

func (p *Peer) Store() *PeerStorage {
	return p.peerStorage
}

func (p *Peer) IsApplyingSnapshot() bool {
	return p.Store().IsApplyingSnapshot()
}

/// Returns `true` if the raft group has replicated a snapshot but not committed it yet.
func (p *Peer) HasPendingSnapshot() bool {
	return p.RaftGroup.GetSnap() != nil
}

func (p *Peer) Send(trans chan<- *rspb.RaftMessage, msgs []eraftpb.Message) error {
	for _, msg := range msgs {
		msgType := msg.MsgType
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			return err
		}
		switch msgType {
		case eraftpb.MessageType_MsgTimeoutNow:
			// After a leader transfer procedure is triggered, the lease for
			// the old leader may be expired earlier than usual, since a new leader
			// may be elected and the old leader doesn't step down due to
			// network partition from the new leader.
			// For lease safety during leader transfer, transit `leader_lease`
			// to suspect.
			p.leaderLease.Suspect(time.Now())
		default:
		}
	}
	return nil
}

/// Steps the raft message.
func (p *Peer) Step(m *eraftpb.Message) error {
	if p.IsLeader() && m.GetFrom() != InvalidID {
		p.PeerHeartbeats[m.GetFrom()] = time.Now()
		// As the leader we know we are not missing.
		p.leaderMissingTime = nil
	} else if m.GetFrom() == p.LeaderId() {
		// As another role know we're not missing.
		p.leaderMissingTime = nil
	}
	return p.RaftGroup.Step(*m)
}

/// Checks and updates `peer_heartbeats` for the peer.
func (p *Peer) CheckPeers() {
	if !p.IsLeader() {
		if len(p.PeerHeartbeats) > 0 {
			p.PeerHeartbeats = make(map[uint64]time.Time)
		}
		return
	}
	if len(p.PeerHeartbeats) == len(p.Region().GetPeers()) {
		return
	}

	// Insert heartbeats in case that some peers never response heartbeats.
	region := p.Region()
	for _, peer := range region.GetPeers() {
		if _, ok := p.PeerHeartbeats[peer.GetId()]; !ok {
			p.PeerHeartbeats[peer.GetId()] = time.Now()
		}
	}
}

/// Collects all down peers.
func (p *Peer) CollectDownPeers(maxDuration time.Duration) []*pdpb.PeerStats {
	downPeers := make([]*pdpb.PeerStats, 0)
	for _, peer := range p.Region().GetPeers() {
		if peer.GetId() == p.Peer.GetId() {
			continue
		}
		if hb, ok := p.PeerHeartbeats[peer.GetId()]; ok {
			elapsed := time.Since(hb)
			if elapsed > maxDuration {
				stats := &pdpb.PeerStats{
					Peer:        peer,
					DownSeconds: uint64(elapsed.Seconds()),
				}
				downPeers = append(downPeers, stats)
			}
		}
	}
	return downPeers
}

/// Collects all pending peers and update `peers_start_pending_time`.
func (p *Peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	status := p.RaftGroup.Status()
	truncatedIdx := p.Store().truncatedIndex()

	// status.Progress includes learner progress
	for id, progress := range status.Progress {
		if id == p.Peer.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.GetPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

func (p *Peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *Peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.Store().truncatedIndex()
		if progress, ok := p.RaftGroup.Raft.Prs[peerId]; ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

func (p *Peer) CheckStaleState(ctx *PollContext) StaleState {
	if p.IsLeader() {
		// Leaders always have valid state.
		//
		// We update the leader_missing_time in the `func Step`. However one peer region
		// does not send any raft messages, so we have to check and update it before
		// reporting stale states.
		p.leaderMissingTime = nil
		return StaleStateValid
	}
	naivePeer := !p.isInitialized() || p.RaftGroup.Raft.IsLearner
	// Updates the `leader_missing_time` according to the current state.
	//
	// If we are checking this it means we suspect the leader might be missing.
	// Mark down the time when we are called, so we can check later if it's been longer than it
	// should be.
	if p.leaderMissingTime == nil {
		now := time.Now()
		p.leaderMissingTime = &now
		return StaleStateValid
	} else {
		elapsed := time.Since(*p.leaderMissingTime)
		if elapsed >= ctx.Cfg.MaxLeaderMissingDuration {
			// Resets the `leader_missing_time` to avoid sending the same tasks to
			// PD worker continuously during the leader missing timeout.
			now := time.Now()
			p.leaderMissingTime = &now
			return StaleStateToValidate
		} else if elapsed >= ctx.Cfg.AbnormalLeaderMissingDuration && !naivePeer {
			// A peer is considered as in the leader missing state
			// if it's initialized but is isolated from its leader or
			// something bad happens that the raft group can not elect a leader.
			return StaleStateLeaderMissing
		}
		return StaleStateValid
	}
}

func (p *Peer) OnRoleChanged(ctx *PollContext, ready *raft.Ready) {
	ss := ready.SoftState
	if ss != nil {
		if ss.RaftState == raft.StateLeader {
			p.HeartbeatPd(ctx)
		}
		ctx.CoprocessorHost.OnRoleChanged(p.Region(), ss.RaftState)
	}
}

func (p *Peer) ReadyToHandlePendingSnap() bool {
	// If apply worker is still working, written apply state may be overwritten
	// by apply worker. So we have to wait here.
	// Please note that committed_index can't be used here. When applying a snapshot,
	// a stale heartbeat can make the leader think follower has already applied
	// the snapshot, and send remaining log entries, which may increase committed_index.
	return p.LastApplyingIdx == p.Store().AppliedIndex()
}

func (p *Peer) readyToHandleRead() bool {
	// 1. There may be some values that are not applied by this leader yet but the old leader,
	// if applied_index_term isn't equal to current term.
	// 2. There may be stale read if the old leader splits really slow,
	// the new region may already elected a new leader while
	// the old leader still think it owns the splitted range.
	// 3. There may be stale read if a target leader is in another store and
	// applied commit merge, written new values, but the sibling peer in
	// this store does not apply commit merge, so the leader is not ready
	// to read, until the merge is rollbacked.
	return p.Store().appliedIndexTerm == p.Term() && !p.isSplitting() && !p.isMerging()
}

func (p *Peer) isSplitting() bool {
	return p.lastCommittedSplitIdx > p.Store().AppliedIndex()
}

func (p *Peer) isMerging() bool {
	return p.lastCommittedPrepareMergeIdx > p.Store().AppliedIndex() || p.PendingMergeState != nil
}

func (p *Peer) TakeApplyProposals() *RegionProposal {
	if len(p.applyProposals) == 0 {
		return nil
	}
	props := p.applyProposals
	p.applyProposals = nil
	return NewRegionProposal(p.PeerId(), p.regionId, props)
}

func (p *Peer) HandleRaftReadyAppend(ctx *PollContext) {
	if p.PendingRemove {
		return
	}
	if p.Store().CheckApplyingSnap() {
		// If we continue to handle all the messages, it may cause too many messages because
		// leader will send all the remaining messages to this follower, which can lead
		// to full message queue under high load.
		log.Debugf("%v still applying snapshot, skip further handling", p.Tag)
		return
	}

	if len(p.pendingMessages) > 0 {
		messages := p.pendingMessages
		p.pendingMessages = nil
		ctx.needFlushTrans = true
		if err := p.Send(ctx.trans, messages); err != nil {
			log.Warnf("%v clear snapshot pengding messages err: %v", p.Tag, err)
		}
	}

	if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
		log.Debugf("%v [apply_id: %v, last_applying_idx: %v] is not ready to apply snapshot.", p.Tag, p.Store().AppliedIndex(), p.LastApplyingIdx)
		return
	}

	if !p.RaftGroup.HasReadySince(&p.LastApplyingIdx) {
		return
	}

	log.Debugf("%v handle raft ready", p.Tag)

	ready := p.RaftGroup.ReadySince(p.LastApplyingIdx)
	p.OnRoleChanged(ctx, &ready)

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if p.IsLeader() {
		ctx.needFlushTrans = true
		if err := p.Send(ctx.trans, ready.Messages); err != nil {
			log.Warnf("%v leader send message err: %v", p.Tag, err)
		}
		ready.Messages = ready.Messages[:0]
	}

	invokeCtx, err := p.Store().HandleRaftReady(ctx, &ready)
	if err != nil {
		panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
	}
	ctx.ReadyRes = append(ctx.ReadyRes, ReadyICPair{Ready: ready, IC: invokeCtx})
}

func (p *Peer) PostRaftReadyAppend(ctx *PollContext, ready *raft.Ready, invokeCtx InvokeContext) *ApplySnapResult {
	if invokeCtx.hasSnapshot() {
		// When apply snapshot, there is no log applied and not compacted yet.
		p.RaftLogSizeHint = 0
	}

	applySnapResult := p.Store().PostReady(&invokeCtx)
	if applySnapResult != nil && p.Peer.GetIsLearner() {
		// The peer may change from learner to voter after snapshot applied.
		var pr *metapb.Peer
		for _, peer := range p.Region().GetPeers() {
			if peer.GetId() == p.Peer.GetId() {
				pr = &metapb.Peer{
					Id:        peer.Id,
					StoreId:   peer.Id,
					IsLearner: peer.IsLearner,
				}
			}
		}
		if !PeerEqual(pr, p.Peer) {
			log.Infof("%v meta changed in applying snapshot, before %v, after %v", p.Tag, p.Peer, pr)
			p.Peer = pr
		}
	}

	if !p.IsLeader() {
		if p.IsApplyingSnapshot() {
			p.pendingMessages = ready.Messages
			ready.Messages = nil
		} else {
			if err := p.Send(ctx.trans, ready.Messages); err != nil {
				log.Warnf("%v follower send messages err: %v", p.Tag, err)
			}
			ctx.needFlushTrans = true
		}
	}

	if applySnapResult != nil {
		p.Activate(ctx)
	}

	return applySnapResult
}

// Try to renew leader lease.
func (p *Peer) MaybeRenewLeaderLease(ts time.Time) {
	// A non-leader peer should never has leader lease.
	// A splitting leader should not renew its lease.
	// Because we split regions asynchronous, the leader may read stale results
	// if splitting runs slow on the leader.
	// // A merging leader should not renew its lease.
	// Because we merge regions asynchronous, the leader may read stale results
	// if commit merge runs slow on sibling peers.
	if !p.IsLeader() || p.isSplitting() || p.isMerging() {
		return
	}
	p.leaderLease.Renew(ts)
}

func (p *Peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

func (p *Peer) findProposeTime(index, term uint64) *time.Time {
	for {
		meta := p.proposals.PopFront(term)
		if meta == nil {
			return nil
		}
		if meta.Index == index && meta.Term == term {
			return meta.RenewLeaseTime
		}
	}
	return nil
}

func (p *Peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *Peer) Stop() {
	p.Store().CancelApplyingSnap()
}

func (p *Peer) InsertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.Id] = peer
}

func (p *Peer) RemovePeerFromCache(peerId uint64) {
	delete(p.peerCache, peerId)
}

func (p *Peer) GetPeerFromCache(peerId uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerId]; ok {
		return &metapb.Peer{
			Id:        peer.Id,
			StoreId:   peer.StoreId,
			IsLearner: peer.IsLearner,
		}
	}

	// Try to find in region, if found, set in cache.
	for _, peer := range p.Region().Peers {
		if peer.Id == peerId {
			p.peerCache[peerId] = &metapb.Peer{
				Id:        peer.Id,
				StoreId:   peer.StoreId,
				IsLearner: peer.IsLearner,
			}
			return &metapb.Peer{
				Id:        peer.Id,
				StoreId:   peer.StoreId,
				IsLearner: peer.IsLearner,
			}
		}
	}
	return nil
}

func (p *Peer) HeartbeatPd(ctx *PollContext) {
	// Todo: currently it is a place holder
}

func (p *Peer) sendRaftMessage(msg eraftpb.Message, trans chan<- *rspb.RaftMessage) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Peer
	toPeer := p.GetPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer.Id, toPeer.Id)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if p.Store().isInitialized() && isInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg

	trans <- sendMsg
	return nil
}
