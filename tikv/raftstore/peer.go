package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
)

type ReadyICPair struct {
	Ready raft.Ready
	IC    *InvokeContext
}

type StaleState int

const (
	StaleStateValid StaleState = 0 + iota
	StaleStateToValidate
	StaleStateLeaderMissing
)

type ReqCbPair struct {
	Req *raft_cmdpb.RaftCmdRequest
	Cb  *Callback
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

func (r *ReadIndexRequest) binaryId() []byte {
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

func NotifyStaleReq(term uint64, cb *Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *Callback) {
	regionNotFound := &ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
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
		read.cmds = nil
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

const (
	ProposalContext_SyncLog      ProposalContext = 1
	ProposalContext_Split        ProposalContext = 1 << 1
	ProposalContext_PrepareMerge ProposalContext = 1 << 2
)

type ProposalContext byte

func (c ProposalContext) ToBytes() []byte {
	return []byte{byte(c)}
}

func NewProposalContextFromBytes(ctx []byte) *ProposalContext {
	var res ProposalContext
	l := len(ctx)
	if l == 0 {
		return nil
	} else if l == 1 {
		res = ProposalContext(ctx[0])
	} else {
		panic(fmt.Sprintf("Invalid ProposalContext %v", ctx))
	}
	return &res
}

func (c *ProposalContext) contains(flag ProposalContext) bool {
	return byte(*c)&byte(flag) != 0
}

func (c *ProposalContext) insert(flag ProposalContext) {
	*c |= flag
}

type PeerStat struct {
	WrittenBytes uint64
	WrittenKeys  uint64
}

/// A struct that stores the state to wait for `PrepareMerge` apply result.
///
/// When handling the apply result of a `CommitMerge`, the source peer may have
/// not handle the apply result of the `PrepareMerge`, so the target peer has
/// to abort current handle process and wait for it asynchronously.
type WaitApplyResultState struct {
	/// The following apply results waiting to be handled, including the `CommitMerge`.
	/// These will be handled once `ReadyToMerge` is true.
	results []*applyTaskRes
	/// It is used by target peer to check whether the apply result of `PrepareMerge` is handled.
	readyToMerge *uint32
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
	Peer           *metapb.Peer
	regionId       uint64
	RaftGroup      *raft.RawNode
	peerStorage    *PeerStorage
	proposals      *ProposalQueue
	applyProposals []*proposal
	pendingReads   *ReadIndexQueue

	peerCache map[uint64]*metapb.Peer

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
	PendingMergeApplyResult *WaitApplyResultState
	PeerStat                PeerStat
}

func NewPeer(storeId uint64, cfg *Config, engines *Engines, region *metapb.Region, regionSched chan<- task,
	peer *metapb.Peer) (*Peer, error) {
	if peer.GetId() == InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), peer.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, peer.GetId(), tag)
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

func (p *Peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

func (p *Peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

func (p *Peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

/// Register self to applyRouter so that the peer is then usable.
/// Also trigger `RegionChangeEvent::Create` here.
func (p *Peer) Activate(ctx *PollContext) {
	ctx.applyRouter.scheduleTask(p.regionId, NewMsg(MsgTypeApplyRegistration, newRegistration(p)))
	ctx.coprocessorHost.OnRegionChanged(p.Region(), RegionChangeEvent_Create, p.GetRole())
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
			log.Infof("%v stale peer %v is applying snapshot", p.Tag, p.Peer.Id)
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
		Peer:        p.Peer,
	}
}

/// Does the real destroy task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *Peer) Destroy(ctx *PollContext, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(WriteBatch)
	raftWB := new(WriteBatch)
	if err := p.Store().clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	var mergeState *rspb.MergeState
	if p.PendingMergeState != nil {
		mergeState = p.PendingMergeState
	}
	WritePeerState(kvWB, region, rspb.PeerState_Tombstone, mergeState)
	// write kv rocksdb first in case of restart happen between two write
	// Todo: sync = ctx.cfg.sync_log
	if err := kvWB.WriteToKV(ctx.engine.kv); err != nil {
		return err
	}
	if err := raftWB.WriteToRaft(ctx.engine.raft); err != nil {
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
		NotifyReqRegionRemoved(region.Id, proposal.cb)
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
	p.Store().SetRegion(region)

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

func (p *Peer) Send(trans Transport, msgs []eraftpb.Message) error {
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
			if peer := p.getPeerFromCache(id); peer != nil {
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
		if elapsed >= ctx.cfg.MaxLeaderMissingDuration {
			// Resets the `leader_missing_time` to avoid sending the same tasks to
			// PD worker continuously during the leader missing timeout.
			now := time.Now()
			p.leaderMissingTime = &now
			return StaleStateToValidate
		} else if elapsed >= ctx.cfg.AbnormalLeaderMissingDuration && !naivePeer {
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
		ctx.coprocessorHost.OnRoleChanged(p.Region(), ss.RaftState)
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

func (p *Peer) TakeApplyProposals() *regionProposal {
	if len(p.applyProposals) == 0 {
		return nil
	}
	props := p.applyProposals
	p.applyProposals = nil
	return newRegionProposal(p.PeerId(), p.regionId, props)
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

	if p.peerStorage.genSnapTask != nil {
		ctx.applyRouter.scheduleTask(p.regionId, Msg{
			Type: MsgTypeApplySnapshot,
			Data: p.peerStorage.genSnapTask,
		})
		p.peerStorage.genSnapTask = nil
	}

	if !p.RaftGroup.HasReadySince(&p.LastApplyingIdx) {
		return
	}

	log.Debugf("%v handle raft ready", p.Tag)

	ready := p.RaftGroup.ReadySince(p.LastApplyingIdx)
	// TODO: workaround for:
	//   in kvproto/eraftpb, we use *SnapshotMetadata
	//   but in etcd, they use SnapshotMetadata
	if ready.Snapshot.GetMetadata() == nil {
		ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
	}
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

func (p *Peer) PostRaftReadyAppend(ctx *PollContext, ready *raft.Ready, invokeCtx *InvokeContext) *ApplySnapResult {
	if invokeCtx.hasSnapshot() {
		// When apply snapshot, there is no log applied and not compacted yet.
		p.RaftLogSizeHint = 0
	}

	applySnapResult := p.Store().PostReady(invokeCtx)
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
}

func (p *Peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *Peer) Stop() {
	p.Store().CancelApplyingSnap()
}

func (p *Peer) HeartbeatPd(ctx *PollContext) {
	ctx.pdScheduler <- task{
		tp: taskTypePDHeartbeat,
		data: &pdRegionHeartbeatTask{
			region:          p.Region(),
			peer:            p.Peer,
			downPeers:       p.CollectDownPeers(time.Minute * 5),
			pendingPeers:    p.CollectPendingPeers(),
			writtenBytes:    p.PeerStat.WrittenBytes,
			writtenKeys:     p.PeerStat.WrittenKeys,
			approximateSize: p.ApproximateSize,
			approximateKeys: p.ApproximateKeys,
		},
	}
}

func (p *Peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Peer
	toPeer := p.getPeerFromCache(msg.To)
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
	return trans.Send(sendMsg)
}

func (p *Peer) HandleRaftReadyApply(ctx *PollContext, ready *raft.Ready) {
	// Call `HandleRaftCommittedEntries` directly here may lead to inconsistency.
	// In some cases, there will be some pending committed entries when applying a
	// snapshot. If we call `HandleRaftCommittedEntries` directly, these updates
	// will be written to disk. Because we apply snapshot asynchronously, so these
	// updates will soon be removed. But the soft state of raft is still be updated
	// in memory. Hence when handle ready next time, these updates won't be included
	// in `ready.committed_entries` again, which will lead to inconsistency.
	if p.IsApplyingSnapshot() {
		// Snapshot's metadata has been applied.
		p.LastApplyingIdx = p.Store().truncatedIndex()
	} else {
		committedEntries := ready.CommittedEntries
		ready.CommittedEntries = nil
		// leader needs to update lease and last commited split index.
		leaseToBeUpdated, splitToBeUpdated, mergeToBeUpdated := p.IsLeader(), p.IsLeader(), p.IsLeader()
		if !leaseToBeUpdated {
			// It's not leader anymore, we are safe to clear proposals. If it becomes leader
			// again, the lease should be updated when election is finished, old proposals
			// have no effect.
			p.proposals.Clear()
		}
		for _, entry := range committedEntries {
			// raft meta is very small, can be ignored.
			p.RaftLogSizeHint += uint64(len(entry.Data))
			if leaseToBeUpdated {
				proposeTime := p.findProposeTime(entry.Index, entry.Term)
				if proposeTime != nil {
					p.MaybeRenewLeaderLease(*proposeTime)
					leaseToBeUpdated = false
				}
			}

			// We care about split/merge commands that are committed in the current term.
			if entry.Term == p.Term() && (splitToBeUpdated || mergeToBeUpdated) {
				//ctx := NewProposalContextFromBytes(entry.Context)
				proposalCtx := NewProposalContextFromBytes([]byte{0})
				if splitToBeUpdated && proposalCtx.contains(ProposalContext_Split) {
					// We dont need to suspect its lease because peers of new region that
					// in other store do not start election before theirs election timeout
					// which is longer than the max leader lease.
					// It's safe to read local within its current lease, however, it's not
					// safe to renew its lease.
					p.lastCommittedSplitIdx = entry.Index
					splitToBeUpdated = false
				}
				if mergeToBeUpdated && proposalCtx.contains(ProposalContext_PrepareMerge) {
					// We committed prepare merge, to prevent unsafe read index,
					// we must record its index.
					p.lastCommittedPrepareMergeIdx = entry.Index
					// After prepare_merge is committed, the leader can not know
					// when the target region merges majority of this region, also
					// it can not know when the target region writes new values.
					// To prevent unsafe local read, we suspect its leader lease.
					p.leaderLease.Suspect(time.Now())
					mergeToBeUpdated = false
				}
			}
		}

		l := len(committedEntries)
		if l > 0 {
			p.LastApplyingIdx = committedEntries[l-1].Index
			if p.LastApplyingIdx >= p.lastUrgentProposalIdx {
				// Urgent requests are flushed, make it lazy again.
				p.RaftGroup.SkipBcastCommit(true)
				p.lastUrgentProposalIdx = math.MaxUint64
			}
			apply := &apply{
				regionId: p.regionId,
				term:     p.Term(),
				entries:  committedEntries,
			}
			ctx.applyRouter.scheduleTask(p.regionId, newApplyMsg(apply))
		}
	}

	p.ApplyReads(ctx, ready)

	p.RaftGroup.Advance(*ready)
	if p.IsApplyingSnapshot() {
		// Because we only handle raft ready when not applying snapshot, so following
		// line won't be called twice for the same snapshot.
		p.RaftGroup.AdvanceApply(p.LastApplyingIdx)
	}
}

func (p *Peer) ApplyReads(ctx *PollContext, ready *raft.Ready) {
	var proposeTime *time.Time
	if p.readyToHandleRead() {
		for _, state := range ready.ReadStates {
			read := p.pendingReads.PopFront()
			if read == nil {
				panic("read should exist")
			}
			if bytes.Compare(state.RequestCtx, read.binaryId()) != 0 {
				panic(fmt.Sprintf("request ctx: %v not equal to read id: %v", state.RequestCtx, read.binaryId()))
			}
			for _, reqCb := range read.cmds {
				resp, _ := p.handleRead(ctx, reqCb.Req, true)
				reqCb.Cb.Done(resp)
			}
			read.cmds = nil
			proposeTime = read.renewLeaseTime
		}
	} else {
		for _, state := range ready.ReadStates {
			read := p.pendingReads.reads[p.pendingReads.readyCnt]
			if bytes.Compare(state.RequestCtx, read.binaryId()) != 0 {
				panic(fmt.Sprintf("request ctx: %v not equal to read id: %v", state.RequestCtx, read.binaryId()))
			}
			p.pendingReads.readyCnt += 1
			proposeTime = read.renewLeaseTime
		}
	}

	// Note that only after handle read_states can we identify what requests are
	// actually stale.
	if ready.SoftState != nil {
		// all uncommitted reads will be dropped silently in raft.
		p.pendingReads.ClearUncommitted(p.Term())
	}

	if proposeTime != nil {
		// `propose_time` is a placeholder, here cares about `Suspect` only,
		// and if it is in `Suspect` phase, the actual timestamp is useless.
		if p.leaderLease.Inspect(proposeTime) == LeaseState_Suspect {
			return
		}
		p.MaybeRenewLeaderLease(*proposeTime)
	}
}

func (p *Peer) PostApply(ctx *PollContext, applyState applyState, appliedIndexTerm uint64, merged bool, applyMetrics applyMetrics) bool {
	hasReady := false
	if p.IsApplyingSnapshot() {
		panic("should not applying snapshot")
	}

	if !merged {
		p.RaftGroup.AdvanceApply(applyState.appliedIndex)
	}

	p.Store().applyState = applyState
	p.Store().appliedIndexTerm = appliedIndexTerm

	p.PeerStat.WrittenBytes += applyMetrics.writtenBytes
	p.PeerStat.WrittenKeys += applyMetrics.writtenKeys
	p.deleteKeysHint += applyMetrics.deleteKeysHint
	diff := p.SizeDiffHint + applyMetrics.sizeDiffHint
	if diff > 0 {
		p.SizeDiffHint = diff
	} else {
		p.SizeDiffHint = 0
	}

	if p.HasPendingSnapshot() && p.ReadyToHandlePendingSnap() {
		hasReady = true
	}

	if p.pendingReads.readyCnt > 0 && p.readyToHandleRead() {
		for i := 0; i < p.pendingReads.readyCnt; i++ {
			read := p.pendingReads.PopFront()
			if read == nil {
				panic("read is nil, this should not happen")
			}
			for _, reqCb := range read.cmds {
				resp, _ := p.handleRead(ctx, reqCb.Req, true)
				reqCb.Cb.Done(resp)
			}
			read.cmds = read.cmds[:0]
		}
		p.pendingReads.readyCnt = 0
	}

	return hasReady
}

func (p *Peer) PostSplit() {
	// Reset delete_keys_hint and size_diff_hint.
	p.deleteKeysHint = 0
	p.SizeDiffHint = 0
}

// Propose a request.
//
// Return true means the request has been proposed successfully.
func (p *Peer) Propose(ctx *PollContext, cb *Callback, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
	if p.PendingRemove {
		return false
	}

	isConfChange := false
	isUrgent := IsUrgentRequest(req)

	policy, err := p.inspect(req)
	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}
	var idx uint64
	switch policy {
	case RequestPolicy_ReadLocal:
		p.readLocal(ctx, req, cb)
		return false
	case RequestPolicy_ReadIndex:
		return p.readIndex(ctx, req, errResp, cb)
	case RequestPolicy_ProposeNormal:
		idx, err = p.ProposeNormal(ctx, req)
	case RequestPolicy_ProposeTransferLeader:
		return p.ProposeTransferLeader(ctx, req, cb)
	case RequestPolicy_ProposeConfChange:
		isConfChange = true
		idx, err = p.ProposeConfChange(ctx, req)
	}

	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}

	if isUrgent {
		p.lastUrgentProposalIdx = idx
		// Eager flush to make urgent proposal be applied on all nodes as soon as
		// possible.
		p.RaftGroup.SkipBcastCommit(false)
	}
	meta := &ProposalMeta{
		Index:          idx,
		Term:           p.Term(),
		RenewLeaseTime: nil,
	}
	p.PostPropose(meta, isConfChange, cb)
	return true
}

func (p *Peer) PostPropose(meta *ProposalMeta, isConfChange bool, cb *Callback) {
	// Try to renew leader lease on every consistent read/write request.
	t := time.Now()
	meta.RenewLeaseTime = &t
	proposal := &proposal{
		isConfChange: isConfChange,
		index:        meta.Index,
		term:         meta.Term,
		cb:           cb,
	}
	p.applyProposals = append(p.applyProposals, proposal)
	p.proposals.Push(meta)
}

/// Count the number of the healthy nodes.
/// A node is healthy when
/// 1. it's the leader of the Raft group, which has the latest logs
/// 2. it's a follower, and it does not lag behind the leader a lot.
///    If a snapshot is involved between it and the Raft leader, it's not healthy since
///    it cannot works as a node in the quorum to receive replicating logs from leader.
func (p *Peer) countHealthyNode(progress map[uint64]raft.Progress) int {
	healthy := 0
	for _, pr := range progress {
		if pr.Match >= p.Store().truncatedIndex() {
			healthy += 1
		}
	}
	return healthy
}

/// Validate the `ConfChange` request and check whether it's safe to
/// propose the specified conf change request.
/// It's safe iff at least the quorum of the Raft group is still healthy
/// right after that conf change is applied.
/// Define the total number of nodes in current Raft cluster to be `total`.
/// To ensure the above safety, if the cmd is
/// 1. A `AddNode` request
///    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
/// 2. A `RemoveNode` request
///    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
///    need to be up to date for now. If 'allow_remove_leader' is false then
///    the peer to be removed should not be the leader.
func (p *Peer) checkConfChange(ctx *PollContext, cmd *raft_cmdpb.RaftCmdRequest) error {
	changePeer := GetChangePeerCmd(cmd)
	changeType := changePeer.GetChangeType()
	peer := changePeer.GetPeer()

	// Check the request itself is valid or not.
	if (changeType == eraftpb.ConfChangeType_AddNode && peer.IsLearner) ||
		(changeType == eraftpb.ConfChangeType_AddLearnerNode && !peer.IsLearner) {
		log.Warnf("%s conf change type: %v, but got peer %v", p.Tag, changeType, peer)
		return fmt.Errorf("invalid conf change request")
	}

	if changeType == eraftpb.ConfChangeType_RemoveNode && !ctx.cfg.AllowRemoveLeader && peer.Id == p.PeerId() {
		log.Warnf("%s rejects remove leader request %v", p.Tag, changePeer)
		return fmt.Errorf("ignore remove leader")
	}

	status := p.RaftGroup.Status()
	total := len(status.Progress)
	if total == 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		if pr, ok := status.Progress[peer.Id]; ok && pr.IsLearner {
			// For promote learner to voter.
			pr.IsLearner = false
			status.Progress[peer.Id] = pr
		} else {
			status.Progress[peer.Id] = raft.Progress{}
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if peer.GetIsLearner() {
			// If the node is a learner, we can return directly.
			return nil
		}
		if _, ok := status.Progress[peer.Id]; ok {
			delete(status.Progress, peer.Id)
		} else {
			// It's always safe to remove a not existing node.
			return nil
		}
	case eraftpb.ConfChangeType_AddLearnerNode:
		return nil
	}

	healthy := p.countHealthyNode(status.Progress)
	quorumAfterChange := Quorum(len(status.Progress))
	if healthy >= quorumAfterChange {
		return nil
	}

	log.Infof("%v rejects unsafe conf chagne request %v, total %v, healthy %v, "+
		"quorum after change %v", p.Tag, changePeer, total, healthy, quorumAfterChange)

	return fmt.Errorf("unsafe to perform conf change %v, total %v, healthy %v, quorum after chagne %v",
		changePeer, total, healthy, quorumAfterChange)
}

func Quorum(total int) int {
	return total/2 + 1
}

func (p *Peer) transferLeader(peer *metapb.Peer) {
	log.Infof("%v transfer leader to %v", p.Tag, peer)

	p.RaftGroup.TransferLeader(peer.GetId())
}

func (p *Peer) readyToTransferLeader(ctx *PollContext, peer *metapb.Peer) bool {
	peerId := peer.GetId()
	status := p.RaftGroup.Status()

	if _, ok := status.Progress[peerId]; !ok {
		return false
	}

	for _, pr := range status.Progress {
		if pr.State == raft.ProgressStateSnapshot {
			return false
		}
	}
	if p.RecentAddedPeer.Contains(peerId) {
		log.Debugf("%v reject tranfer leader to %v due to the peer was added recently", p.Tag, peer)
		return false
	}

	lastIndex, _ := p.Store().LastIndex()

	return lastIndex <= status.Progress[peerId].Match+ctx.cfg.LeaderTransferMaxLogLag
}

func (p *Peer) readLocal(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, cb *Callback) {
	resp, _ := p.handleRead(ctx, req, false)
	cb.Done(resp)
}

func (p *Peer) preReadIndex() error {
	// See more in ReadyToHandleRead().
	if p.isSplitting() {
		return fmt.Errorf("can not read index due to split")
	}
	if p.isMerging() {
		return fmt.Errorf("can not read index due to merge")
	}
	return nil
}

// Returns a boolean to indicate whether the `read` is proposed or not.
// For these cases it won't be proposed:
// 1. The region is in merging or splitting;
// 2. The message is stale and dropped by the Raft group internally;
// 3. There is already a read request proposed in the current lease;
func (p *Peer) readIndex(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse, cb *Callback) bool {
	err := p.preReadIndex()
	if err != nil {
		log.Debugf("%v prevents unsafe read index, err: %v", p.Tag, err)
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}

	now := time.Now()
	renewLeaseTime := &now
	readsLen := len(p.pendingReads.reads)
	if readsLen > 0 {
		read := p.pendingReads.reads[readsLen-1]
		if read.renewLeaseTime.Add(pollCtx.cfg.RaftStoreMaxLeaderLease).After(*renewLeaseTime) {
			read.cmds = append(read.cmds, &ReqCbPair{Req: req, Cb: cb})
			return false
		}
	}

	lastPendingReadCount := p.RaftGroup.Raft.PendingReadCount()
	lastReadyReadCount := p.RaftGroup.Raft.ReadyReadCount()

	id := p.pendingReads.NextId()
	ctx := make([]byte, 8)
	binary.BigEndian.PutUint64(ctx, id)
	p.RaftGroup.ReadIndex(ctx)

	pendingReadCount := p.RaftGroup.Raft.PendingReadCount()
	readyReadCount := p.RaftGroup.Raft.ReadyReadCount()

	if pendingReadCount == lastPendingReadCount && readyReadCount == lastReadyReadCount {
		// The message gets dropped silently, can't be handled anymore.
		NotifyStaleReq(p.Term(), cb)
		return false
	}

	cmds := []*ReqCbPair{&ReqCbPair{req, cb}}
	p.pendingReads.reads = append(p.pendingReads.reads, NewReadIndexRequest(id, cmds, renewLeaseTime))

	// TimeoutNow has been sent out, so we need to propose explicitly to
	// update leader lease.
	if p.leaderLease.Inspect(renewLeaseTime) == LeaseState_Suspect {
		req := new(raft_cmdpb.RaftCmdRequest)
		if index, err := p.ProposeNormal(pollCtx, req); err == nil {
			meta := &ProposalMeta{
				Index:          index,
				Term:           p.Term(),
				RenewLeaseTime: renewLeaseTime,
			}
			p.PostPropose(meta, false, NewCallback())
		}
	}

	return true
}

func (p *Peer) GetMinProgress() uint64 {
	var minMatch uint64 = math.MaxUint64
	hasProgress := false
	for _, pr := range p.RaftGroup.Status().Progress {
		hasProgress = true
		if pr.Match < minMatch {
			minMatch = pr.Match
		}
	}
	if !hasProgress {
		return 0
	}
	return minMatch
}

func (p *Peer) preProposePrepareMerge(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest) error {
	lastIndex := p.RaftGroup.Raft.RaftLog.LastIndex()
	minProgress := p.GetMinProgress()
	minIndex := minProgress + 1
	if minProgress == 0 || lastIndex-minProgress > ctx.cfg.MergeMaxLogGap {
		return fmt.Errorf("log gap (%v, %v] is too large, skip merge", minProgress, lastIndex)
	}

	entrySize := 0

	ents, err := p.RaftGroup.Raft.RaftLog.Entries(minIndex, math.MaxUint64)
	if err != nil {
		return err
	}
	for _, entry := range ents {
		entrySize += len(entry.Data)
		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			return fmt.Errorf("log gap contains conf change, skip merging.")
		}
		if len(entry.Data) == 0 {
			continue
		}
		cmd := raft_cmdpb.RaftCmdRequest{}
		err := cmd.Unmarshal(entry.Data)
		if err != nil {
			panic(fmt.Sprintf("%v data is corrupted at %v, error: %v", p.Tag, entry.Index, err))
		}
		if cmd.AdminRequest == nil {
			continue
		}
		cmdType := cmd.AdminRequest.GetCmdType()
		switch cmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader, raft_cmdpb.AdminCmdType_ComputeHash,
			raft_cmdpb.AdminCmdType_VerifyHash, raft_cmdpb.AdminCmdType_InvalidAdmin:
			continue
		default:
		}

		// Any command that can change epoch or log gap should be rejected.
		return fmt.Errorf("log gap contains admin request %v, skip merging.", cmdType)
	}

	if float64(entrySize) > float64(ctx.cfg.RaftEntryMaxSize)*0.9 {
		return fmt.Errorf("log gap size exceed entry size limit, skip merging.")
	}

	req.AdminRequest.PrepareMerge.MinIndex = minIndex
	return nil
}

func (p *Peer) PrePropose(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest) (*ProposalContext, error) {
	if err := pollCtx.coprocessorHost.PrePropose(p.Region(), req); err != nil {
		return nil, err
	}
	ctx := new(ProposalContext)

	if getSyncLogFromRequest(req) {
		ctx.insert(ProposalContext_SyncLog)
	}

	if req.AdminRequest == nil {
		return ctx, nil
	}

	switch req.AdminRequest.GetCmdType() {
	case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit:
		ctx.insert(ProposalContext_Split)
	default:
	}

	if req.AdminRequest.PrepareMerge != nil {
		err := p.preProposePrepareMerge(pollCtx, req)
		if err != nil {
			return nil, err
		}
		ctx.insert(ProposalContext_PrepareMerge)
	}

	return ctx, nil
}

func (p *Peer) ProposeNormal(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.PendingMergeState != nil && (req.AdminRequest.GetCmdType() != raft_cmdpb.AdminCmdType_RollbackMerge) {
		return 0, fmt.Errorf("peer in merging mode, can't do proposal.")
	}

	// TODO: validate request for unexpected changes.
	ctx, err := p.PrePropose(pollCtx, req)
	if err != nil {
		log.Warnf("%v skip proposal: %v", p.Tag, err)
		return 0, err
	}
	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	if uint64(len(data)) > pollCtx.cfg.RaftEntryMaxSize {
		log.Errorf("entry is too large, entry size %v", len(data))
		return 0, &ErrRaftEntryTooLarge{RegionId: p.regionId, EntrySize: uint64(len(data))}
	}

	proposeIndex := p.nextProposalIndex()
	err = p.RaftGroup.Propose(ctx.ToBytes(), data)
	if err != nil {
		return 0, err
	}
	if proposeIndex == p.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

// Return true if the transfer leader request is accepted.
func (p *Peer) ProposeTransferLeader(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, cb *Callback) bool {
	transferLeader := getTransferLeaderCmd(req)
	peer := transferLeader.Peer

	transferred := false
	if p.readyToTransferLeader(ctx, peer) {
		p.transferLeader(peer)
		transferred = true
	} else {
		log.Infof("%v transfer leader message %v ignored directly", p.Tag, req)
		transferred = false
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	cb.Done(makeTransferLeaderResponse())

	return transferred
}

// Fails in such cases:
// 1. A pending conf change has not been applied yet;
// 2. Removing the leader is not allowed in the configuration;
// 3. The conf change makes the raft group not healthy;
// 4. The conf change is dropped by raft group internally.
func (p *Peer) ProposeConfChange(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.PendingMergeState != nil {
		return 0, fmt.Errorf("peer in merging mode, can't do proposal.")
	}

	if p.RaftGroup.Raft.PendingConfIndex > p.Store().AppliedIndex() {
		log.Infof("%v there is a pending conf change, try later", p.Tag)
		return 0, fmt.Errorf("%v there is a pending conf change, try later", p.Tag)
	}

	if err := p.checkConfChange(ctx, req); err != nil {
		return 0, err
	}

	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	changePeer := GetChangePeerCmd(req)
	var cc eraftpb.ConfChange
	cc.ChangeType = eraftpb.ConfChangeType(int32(changePeer.ChangeType))
	cc.NodeId = changePeer.Peer.Id
	cc.Context = data

	log.Infof("%v propose conf change %v peer %v", p.Tag, cc.ChangeType, cc.NodeId)

	proposeIndex := p.nextProposalIndex()
	var proposalCtx ProposalContext = ProposalContext_SyncLog
	if err = p.RaftGroup.ProposeConfChange(proposalCtx.ToBytes(), cc); err != nil {
		return 0, err
	}
	if p.nextProposalIndex() == proposeIndex {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

func (p *Peer) handleRead(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, checkEpoch bool) (*raft_cmdpb.RaftCmdResponse, *DBSnapshot) {
	readExecutor := NewReadExecutor(ctx.dbBundle, checkEpoch, false)
	resp, snap := readExecutor.Execute(req, p.Region())
	BindRespTerm(resp, p.Term())
	return resp, snap
}

type RequestPolicy int

const (
	// Handle the read request directly without dispatch.
	RequestPolicy_ReadLocal RequestPolicy = 0 + iota
	// Handle the read request via raft's SafeReadIndex mechanism.
	RequestPolicy_ReadIndex
	RequestPolicy_ProposeNormal
	RequestPolicy_ProposeTransferLeader
	RequestPolicy_ProposeConfChange
	RequestPolicy_Invalid
)

type RequestInspector interface {
	// Has the current term been applied?
	hasAppliedToCurrentTerm() bool
	// Inspects its lease.
	inspectLease() LeaseState
	// Inspect a request, return a policy that tells us how to
	// handle the request.
	inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error)
}

func (p *Peer) hasAppliedToCurrentTerm() bool {
	return p.Store().appliedIndexTerm == p.Term()
}

func (p *Peer) inspectLease() LeaseState {
	if !p.RaftGroup.Raft.InLease() {
		return LeaseState_Suspect
	}
	// nil means now.
	state := p.leaderLease.Inspect(nil)
	if state == LeaseState_Expired {
		log.Debugf("%v leader lease is expired %v", p.Tag, p.leaderLease)
		p.leaderLease.Expire()
	}
	return state
}

func (p *Peer) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	return Inspect(p, req)
}

func Inspect(i RequestInspector, req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	if req.AdminRequest != nil {
		if GetChangePeerCmd(req) != nil {
			return RequestPolicy_ProposeConfChange, nil
		}
		if getTransferLeaderCmd(req) != nil {
			return RequestPolicy_ProposeTransferLeader, nil
		}
		return RequestPolicy_ProposeNormal, nil
	}

	hasRead, hasWrite := false, false
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
			hasRead = true
		case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_DeleteRange,
			raft_cmdpb.CmdType_IngestSST:
			hasWrite = true
		case raft_cmdpb.CmdType_Prewrite, raft_cmdpb.CmdType_Invalid:
			return RequestPolicy_Invalid, fmt.Errorf("invalid cmd type %v, message maybe corrupted", r.CmdType)
		}

		if hasRead && hasWrite {
			return RequestPolicy_Invalid, fmt.Errorf("read and write can't be mixed in one batch.")
		}
	}

	if hasWrite {
		return RequestPolicy_ProposeNormal, nil
	}

	if req.Header != nil && req.Header.ReadQuorum {
		return RequestPolicy_ReadIndex, nil
	}

	// If applied index's term is differ from current raft's term, leader transfer
	// must happened, if read locally, we may read old value.
	if !i.hasAppliedToCurrentTerm() {
		return RequestPolicy_ReadIndex, nil
	}

	// Local read should be performed, if and only if leader is in lease.
	// None for now.
	switch i.inspectLease() {
	case LeaseState_Valid:
		return RequestPolicy_ReadLocal, nil
	case LeaseState_Expired, LeaseState_Suspect:
		// Perform a consistent read to Raft quorum and try to renew the leader lease.
		return RequestPolicy_ReadIndex, nil
	}
	return RequestPolicy_ReadLocal, nil
}

type ReadExecutor struct {
	checkEpoch       bool
	engine           *DBBundle
	snapshot         *DBSnapshot
	snapshotTime     *time.Time
	needSnapshotTime bool
}

func NewReadExecutor(engine *DBBundle, checkEpoch bool, needSnapshotTime bool) *ReadExecutor {
	return &ReadExecutor{
		checkEpoch:       checkEpoch,
		engine:           engine,
		snapshot:         nil,
		snapshotTime:     nil,
		needSnapshotTime: needSnapshotTime,
	}
}

func (r *ReadExecutor) SnapshotTime() *time.Time {
	r.MaybeUpdateSnapshot()
	return r.snapshotTime
}

func (r *ReadExecutor) MaybeUpdateSnapshot() {
	if r.snapshot != nil {
		return
	}
	r.snapshot = &DBSnapshot{
		Txn:           r.engine.db.NewTransaction(false),
		LockStore:     r.engine.lockStore,
		RollbackStore: r.engine.rollbackStore,
	}
	// Reading current timespec after snapshot, in case we do not
	// expire lease in time.
	// Todo: atomic::fence(atomic::Ordering::Release)
	if r.needSnapshotTime {
		t := time.Now()
		r.snapshotTime = &t
	}
}

func (r *ReadExecutor) DoGet(req *raft_cmdpb.Request, region *metapb.Region) (*raft_cmdpb.Response, error) {
	// region key range has no data prefix, so we must use origin key to check.
	if err := CheckKeyInRegion(req.Get.Key, region); err != nil {
		return nil, err
	}

	resp := new(raft_cmdpb.Response)
	item, err := r.snapshot.Txn.Get(DataKey(req.Get.Key))
	if err != nil {
		panic(fmt.Sprintf("[region %v] failed to get %v, err: %v", region.Id, req.Get.Key, err))
	}
	if item != nil {
		value, err := item.Value()
		if err != nil {
			panic(fmt.Sprintf("[region %v] failed to get %v, err: %v", region.Id, req.Get.Key, err))
		}
		resp.Get = new(raft_cmdpb.GetResponse)
		resp.Get.Value = append([]byte{}, value...)
	}
	return resp, nil
}

func (r *ReadExecutor) Execute(msg *raft_cmdpb.RaftCmdRequest, region *metapb.Region) (*raft_cmdpb.RaftCmdResponse, *DBSnapshot) {
	if r.checkEpoch {
		if err := CheckRegionEpoch(msg, region, true); err != nil {
			log.Debugf("[region %v] epoch not match, err: %v", region.Id, err)
			return ErrResp(err), nil
		}
	}

	r.MaybeUpdateSnapshot()
	needSnapshot := false
	resps := make([]*raft_cmdpb.Response, 0, len(msg.Requests))
	var resp *raft_cmdpb.Response
	var err error
	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			if resp, err = r.DoGet(req, region); err != nil {
				log.Errorf("[region %v] execute raft command err %v", region.Id, err)
				return ErrResp(err), nil
			}
		case raft_cmdpb.CmdType_Snap:
			needSnapshot = true
			resp = new(raft_cmdpb.Response)
		case raft_cmdpb.CmdType_Prewrite, raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_Delete,
			raft_cmdpb.CmdType_DeleteRange, raft_cmdpb.CmdType_IngestSST, raft_cmdpb.CmdType_Invalid:
			panic("unreachable")
		}

		resp.CmdType = req.CmdType
		resps = append(resps, resp)
	}

	response := new(raft_cmdpb.RaftCmdResponse)
	response.Responses = resps
	if needSnapshot {
		return response, r.snapshot
	} else {
		return response, nil
	}
}

func getTransferLeaderCmd(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.TransferLeaderRequest {
	if req.AdminRequest == nil {
		return nil
	}
	return req.AdminRequest.TransferLeader
}

func getSyncLogFromRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.AdminRequest != nil {
		switch req.AdminRequest.GetCmdType() {
		case raft_cmdpb.AdminCmdType_ChangePeer, raft_cmdpb.AdminCmdType_Split,
			raft_cmdpb.AdminCmdType_BatchSplit, raft_cmdpb.AdminCmdType_PrepareMerge,
			raft_cmdpb.AdminCmdType_CommitMerge, raft_cmdpb.AdminCmdType_RollbackMerge:
			return true
		default:
			return false
		}
	}
	return req.Header.GetSyncLog()
}

/// We enable follower lazy commit to get a better performance.
/// But it may not be appropriate for some requests. This function
/// checks whether the request should be committed on all followers
/// as soon as possible.
func IsUrgentRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.AdminRequest == nil {
		return false
	}
	switch req.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_Split,
		raft_cmdpb.AdminCmdType_BatchSplit,
		raft_cmdpb.AdminCmdType_ChangePeer,
		raft_cmdpb.AdminCmdType_ComputeHash,
		raft_cmdpb.AdminCmdType_VerifyHash,
		raft_cmdpb.AdminCmdType_PrepareMerge,
		raft_cmdpb.AdminCmdType_CommitMerge,
		raft_cmdpb.AdminCmdType_RollbackMerge:
		return true
	default:
		return false
	}
}

func makeTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
	adminResp := &raft_cmdpb.AdminResponse{}
	adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
	adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	resp := &raft_cmdpb.RaftCmdResponse{}
	resp.AdminResponse = adminResp
	return resp
}

func GetChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
	if msg.AdminRequest == nil || msg.AdminRequest.ChangePeer == nil {
		return nil
	}
	return msg.AdminRequest.ChangePeer
}
