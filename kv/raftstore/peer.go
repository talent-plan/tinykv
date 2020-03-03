package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

func (p *peer) drop() {
	p.Stop()
}

func (p *peer) regionID() uint64 {
	return p.regionId
}

func (p *peer) region() *metapb.Region {
	return p.Store().region
}

func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

func (p *peer) peerID() uint64 {
	return p.Meta.Id
}

func (p *peer) stop() {
	p.stopped = true
}

func (p *peer) scheduleApplyingSnapshot() {
	p.Store().ScheduleApplyingSnapshot()
}

func (p *peer) tag() string {
	return p.Tag
}

type peer struct {
	stopped  bool
	hasReady bool
	ticker   *ticker

	Meta           *metapb.Peer
	regionId       uint64
	RaftGroup      *raft.RawNode
	peerStorage    *PeerStorage
	applyProposals []*proposal

	peerCache map[uint64]*metapb.Peer

	// Record the last instant of each peer's heartbeat response.
	PeerHeartbeats map[uint64]time.Time

	/// Record the instants of peers being added into the configuration.
	/// Remove them after they are not pending any more.
	PeersStartPendingTime map[uint64]time.Time

	/// an inaccurate difference in region size since last reset.
	SizeDiffHint uint64
	/// approximate size of the region.
	ApproximateSize *uint64

	Tag string

	// Index of last scheduled committed raft log.
	LastApplyingIdx  uint64
	LastCompactedIdx uint64

	PendingRemove bool

	// If a snapshot is being applied asynchronously, messages should not be sent.
	pendingMessages []eraftpb.Message
}

func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, meta.GetId(), tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeerHeartbeats:        make(map[uint64]time.Time),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		LastApplyingIdx:       appliedIndex,
		ticker:                newTicker(region.GetId(), cfg),
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

func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
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

func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
func (p *peer) MaybeDestroy() bool {
	if p.PendingRemove {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	if p.IsApplyingSnapshot() {
		if !p.Store().CancelApplyingSnap() {
			log.Infof("%v stale peer %v is applying snapshot", p.Tag, p.Meta.Id)
			return false
		}
	}
	p.PendingRemove = true
	return true
}

/// Does the real destroy worker.Task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.Store().clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	WritePeerState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv rocksdb first in case of restart happen between two write
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.Store().isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		if err := p.Store().ClearData(); err != nil {
			log.Errorf("%v failed to schedule clear data worker.Task %v", p.Tag, err)
		}
	}

	for _, proposal := range p.applyProposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.applyProposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// Set the region of a peer.
///
/// This will update the region of the peer, caller must ensure the region
/// has been preserved in a durable device.
func (p *peer) SetRegion(region *metapb.Region) {
	p.Store().SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

func (p *peer) GetRole() raft.StateType {
	return p.RaftGroup.Raft.State
}

func (p *peer) Store() *PeerStorage {
	return p.peerStorage
}

func (p *peer) IsApplyingSnapshot() bool {
	return p.Store().IsApplyingSnapshot()
}

/// Returns `true` if the raft group has replicated a snapshot but not committed it yet.
func (p *peer) HasPendingSnapshot() bool {
	return p.RaftGroup.GetSnap() != nil
}

func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

/// Steps the raft message.
func (p *peer) Step(m *eraftpb.Message) error {
	if p.IsLeader() && m.GetFrom() != util.InvalidID {
		p.PeerHeartbeats[m.GetFrom()] = time.Now()
	}
	return p.RaftGroup.Step(*m)
}

/// Checks and updates `peer_heartbeats` for the peer.
func (p *peer) CheckPeers() {
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
func (p *peer) CollectDownPeers(maxDuration time.Duration) []*pdpb.PeerStats {
	downPeers := make([]*pdpb.PeerStats, 0)
	for _, peer := range p.Region().GetPeers() {
		if peer.GetId() == p.Meta.GetId() {
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
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.Store().truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
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

func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.Store().truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
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

func (p *peer) ReadyToHandlePendingSnap() bool {
	// If apply worker is still working, written apply state may be overwritten
	// by apply worker. So we have to wait here.
	// Please note that committed_index can't be used here. When applying a snapshot,
	// a stale heartbeat can make the leader think follower has already applied
	// the snapshot, and send remaining log entries, which may increase committed_index.
	return p.LastApplyingIdx == p.Store().AppliedIndex()
}

func (p *peer) TakeApplyProposals() *MsgApplyProposal {
	if len(p.applyProposals) == 0 {
		return nil
	}
	props := p.applyProposals
	p.applyProposals = nil
	return &MsgApplyProposal{
		Id:       p.PeerId(),
		RegionId: p.regionId,
		Props:    props,
	}
}

func (p *peer) HandleRaftReady(msgs []message.Msg, pdScheduler chan<- worker.Task, trans Transport) (*ApplySnapResult, []message.Msg) {
	if p.PendingRemove {
		return nil, msgs
	}
	if p.Store().CheckApplyingSnap() {
		// If we continue to handle all the messages, it may cause too many messages because
		// leader will send all the remaining messages to this follower, which can lead
		// to full message queue under high load.
		log.Debugf("%v still applying snapshot, skip further handling", p.Tag)
		return nil, msgs
	}

	if len(p.pendingMessages) > 0 {
		messages := p.pendingMessages
		p.pendingMessages = nil
		p.Send(trans, messages)
	}

	if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
		log.Debugf("%v [apply_id: %v, last_applying_idx: %v] is not ready to apply snapshot.", p.Tag, p.Store().AppliedIndex(), p.LastApplyingIdx)
		return nil, msgs
	}

	if !p.RaftGroup.HasReadySince(p.LastApplyingIdx) {
		return nil, msgs
	}

	log.Debugf("%v handle raft ready", p.Tag)

	ready := p.RaftGroup.ReadySince(p.LastApplyingIdx)
	// TODO: workaround for:
	//   in kvproto/eraftpb, we use *SnapshotMetadata
	//   but in etcd, they use SnapshotMetadata
	if ready.Snapshot.GetMetadata() == nil {
		ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if p.IsLeader() {
		p.Send(trans, ready.Messages)
		ready.Messages = ready.Messages[:0]
	}
	ss := ready.SoftState
	if ss != nil && ss.RaftState == raft.StateLeader {
		p.HeartbeatPd(pdScheduler)
	}

	applySnapResult, err := p.Store().SaveReadyState(&ready)
	if err != nil {
		panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
	}
	if !p.IsLeader() {
		if p.IsApplyingSnapshot() {
			p.pendingMessages = ready.Messages
			ready.Messages = nil
		} else {
			p.Send(trans, ready.Messages)
		}
	}

	if applySnapResult != nil {
		/// Register self to applyMsgs so that the peer is then usable.
		msgs = append(msgs, message.NewPeerMsg(message.MsgTypeApplyRefresh, p.regionId, &MsgApplyRefresh{
			id:               p.PeerId(),
			term:             p.Term(),
			applyState:       p.Store().applyState,
			appliedIndexTerm: p.Store().appliedIndexTerm,
			region:           p.Region(),
		}))

		// Snapshot's metadata has been applied.
		p.LastApplyingIdx = p.Store().truncatedIndex()
	} else {
		committedEntries := ready.CommittedEntries
		ready.CommittedEntries = nil
		l := len(committedEntries)
		if l > 0 {
			p.LastApplyingIdx = committedEntries[l-1].Index
			msgs = append(msgs, message.Msg{Type: message.MsgTypeApplyCommitted, Data: &MsgApplyCommitted{
				regionId: p.regionId,
				term:     p.Term(),
				entries:  committedEntries,
			}, RegionID: p.regionId})
		}
	}

	p.RaftGroup.Advance(ready)
	if applySnapResult != nil {
		// Because we only handle raft ready when not applying snapshot, so following
		// line won't be called twice for the same snapshot.
		p.RaftGroup.AdvanceApply(p.LastApplyingIdx)
	}
	return applySnapResult, msgs
}

func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) Stop() {
	p.Store().CancelApplyingSnap()
}

func (p *peer) HeartbeatPd(pdScheduler chan<- worker.Task) {
	pdScheduler <- worker.Task{
		Tp: worker.TaskTypePDHeartbeat,
		Data: &runner.PdRegionHeartbeatTask{
			Region:          p.Region(),
			Peer:            p.Meta,
			DownPeers:       p.CollectDownPeers(time.Minute * 5),
			PendingPeers:    p.CollectPendingPeers(),
			ApproximateSize: p.ApproximateSize,
		},
	}
}

func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
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
	if p.Store().isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}

func (p *peer) PostApply(kv *badger.DB, applyState rspb.RaftApplyState, appliedIndexTerm uint64, sizeDiffHint uint64) bool {
	hasReady := false
	if p.IsApplyingSnapshot() {
		panic("should not applying snapshot")
	}
	p.RaftGroup.AdvanceApply(applyState.AppliedIndex)

	p.Store().applyState = applyState
	p.Store().appliedIndexTerm = appliedIndexTerm

	diff := p.SizeDiffHint + sizeDiffHint
	if diff > 0 {
		p.SizeDiffHint = diff
	} else {
		p.SizeDiffHint = 0
	}

	if p.HasPendingSnapshot() && p.ReadyToHandlePendingSnap() {
		hasReady = true
	}

	return hasReady
}

// Propose a request.
//
// Return true means the request has been proposed successfully.
func (p *peer) Propose(kv *badger.DB, cfg *config.Config, cb *message.Callback, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
	if p.PendingRemove {
		return false
	}

	isConfChange := false

	policy, err := p.inspect(req)
	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}
	var idx uint64
	switch policy {
	case RequestPolicy_ProposeNormal:
		idx, err = p.ProposeNormal(cfg, req)
	case RequestPolicy_ProposeTransferLeader:
		return p.ProposeTransferLeader(cfg, req, cb)
	case RequestPolicy_ProposeConfChange:
		isConfChange = true
		idx, err = p.ProposeConfChange(cfg, req)
	}

	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}

	p.PostPropose(idx, p.Term(), isConfChange, cb)
	return true
}

func (p *peer) PostPropose(index, term uint64, isConfChange bool, cb *message.Callback) {
	proposal := &proposal{
		isConfChange: isConfChange,
		index:        index,
		term:         term,
		cb:           cb,
	}
	p.applyProposals = append(p.applyProposals, proposal)
}

/// Count the number of the healthy nodes.
/// A node is healthy when
/// 1. it's the leader of the Raft group, which has the latest logs
/// 2. it's a follower, and it does not lag behind the leader a lot.
///    If a snapshot is involved between it and the Raft leader, it's not healthy since
///    it cannot works as a node in the quorum to receive replicating logs from leader.
func (p *peer) countHealthyNode(progress map[uint64]raft.Progress) int {
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
func (p *peer) checkConfChange(cfg *config.Config, cmd *raft_cmdpb.RaftCmdRequest) error {
	changePeer := GetChangePeerCmd(cmd)
	changeType := changePeer.GetChangeType()
	peer := changePeer.GetPeer()

	progress := p.RaftGroup.GetProgress()
	total := len(progress)
	if total <= 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		progress[peer.Id] = raft.Progress{}
	case eraftpb.ConfChangeType_RemoveNode:
		if _, ok := progress[peer.Id]; ok {
			delete(progress, peer.Id)
		} else {
			// It's always safe to remove a not existing node.
			return nil
		}
	}

	healthy := p.countHealthyNode(progress)
	quorumAfterChange := Quorum(len(progress))
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

func (p *peer) transferLeader(peer *metapb.Peer) {
	log.Infof("%v transfer leader to %v", p.Tag, peer)

	p.RaftGroup.TransferLeader(peer.GetId())
}

func (p *peer) ProposeNormal(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	proposeIndex := p.nextProposalIndex()
	err = p.RaftGroup.Propose(data)
	if err != nil {
		return 0, err
	}
	if proposeIndex == p.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &util.ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

// Return true if the transfer leader request is accepted.
func (p *peer) ProposeTransferLeader(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	transferLeader := getTransferLeaderCmd(req)
	peer := transferLeader.Peer

	p.transferLeader(peer)
	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	cb.Done(makeTransferLeaderResponse())

	return true
}

// Fails in such cases:
// 1. A pending conf change has not been applied yet;
// 2. Removing the leader is not allowed in the configuration;
// 3. The conf change makes the raft group not healthy;
// 4. The conf change is dropped by raft group internally.
func (p *peer) ProposeConfChange(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.RaftGroup.Raft.PendingConfIndex > p.Store().AppliedIndex() {
		log.Infof("%v there is a pending conf change, try later", p.Tag)
		return 0, fmt.Errorf("%v there is a pending conf change, try later", p.Tag)
	}

	if err := p.checkConfChange(cfg, req); err != nil {
		return 0, err
	}

	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	changePeer := GetChangePeerCmd(req)
	var cc eraftpb.ConfChange
	cc.ChangeType = changePeer.ChangeType
	cc.NodeId = changePeer.Peer.Id
	cc.Context = data

	log.Infof("%v propose conf change %v peer %v", p.Tag, cc.ChangeType, cc.NodeId)

	proposeIndex := p.nextProposalIndex()
	if err = p.RaftGroup.ProposeConfChange(cc); err != nil {
		return 0, err
	}
	if p.nextProposalIndex() == proposeIndex {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		return 0, &util.ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

type RequestPolicy int

const (
	RequestPolicy_ProposeNormal RequestPolicy = 0 + iota
	RequestPolicy_ProposeTransferLeader
	RequestPolicy_ProposeConfChange
	RequestPolicy_Invalid
)

func (p *peer) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	if req.AdminRequest != nil {
		if GetChangePeerCmd(req) != nil {
			return RequestPolicy_ProposeConfChange, nil
		}
		if getTransferLeaderCmd(req) != nil {
			return RequestPolicy_ProposeTransferLeader, nil
		}
	}

	hasRead, hasWrite := false, false
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
			hasRead = true
		case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put:
			hasWrite = true
		case raft_cmdpb.CmdType_Invalid:
			return RequestPolicy_Invalid, fmt.Errorf("invalid cmd type %v, message maybe corrupted", r.CmdType)
		}

		if hasRead && hasWrite {
			return RequestPolicy_Invalid, fmt.Errorf("read and write can't be mixed in one request.")
		}
	}
	return RequestPolicy_ProposeNormal, nil
}

func getTransferLeaderCmd(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.TransferLeaderRequest {
	if req.AdminRequest == nil {
		return nil
	}
	return req.AdminRequest.TransferLeader
}

func makeTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
	adminResp := &raft_cmdpb.AdminResponse{}
	adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
	adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	resp.AdminResponse = adminResp
	return resp
}

func GetChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
	if msg.AdminRequest == nil || msg.AdminRequest.ChangePeer == nil {
		return nil
	}
	return msg.AdminRequest.ChangePeer
}
