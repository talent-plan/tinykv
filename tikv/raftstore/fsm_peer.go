package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
)

type peerFsm struct {
	peer     *Peer
	stopped  bool
	hasReady bool
	mailbox  *mailbox
	receiver <-chan Msg
	ticker   *ticker
}

type PeerEventContext struct {
	LeaderChecker LeaderChecker
	RegionId      uint64
}

type PeerEventObserver interface {
	// OnPeerCreate will be invoked when there is a new peer created.
	OnPeerCreate(ctx *PeerEventContext, region *metapb.Region)
	// OnPeerApplySnap will be invoked when there is a replicate peer's snapshot applied.
	OnPeerApplySnap(ctx *PeerEventContext, region *metapb.Region)
	// OnPeerDestroy will be invoked when a peer is destroyed.
	OnPeerDestroy(ctx *PeerEventContext)
	// OnSplitRegion will be invoked when region split into new regions with corresponding peers.
	OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*PeerEventContext)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeerFsm(storeID uint64, cfg *Config, sched chan<- task,
	engines *Engines, region *metapb.Region) (chan<- Msg, *peerFsm, error) {
	metaPeer := findPeer(region, storeID)
	if metaPeer == nil {
		return nil, nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	ch := make(chan Msg, msgDefaultChanSize)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, nil, err
	}
	return (chan<- Msg)(ch), &peerFsm{
		peer:     peer,
		receiver: ch,
		ticker:   newTicker(region.GetId(), cfg),
	}, nil
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeerFsm(storeID uint64, cfg *Config, sched chan<- task,
	engines *Engines, regionID uint64, metaPeer *metapb.Peer) (chan<- Msg, *peerFsm, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	ch := make(chan Msg, msgDefaultChanSize)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, nil, err
	}
	return (chan<- Msg)(ch), &peerFsm{
		peer:     peer,
		receiver: ch,
		ticker:   newTicker(region.GetId(), cfg),
	}, nil
}

func (pf *peerFsm) drop() {
	pf.peer.Stop()
	for {
		select {
		case msg := <-pf.receiver:
			var cb *Callback
			switch msg.Type {
			case MsgTypeRaftCmd:
				cb = msg.Data.(*MsgRaftCmd).Callback
			case MsgTypeSplitRegion:
				cb = msg.Data.(*MsgSplitRegion).Callback
			default:
				continue
			}
			cb.Done(ErrRespRegionNotFound(pf.regionID()))
		default:
			return
		}
	}
}

func (pf *peerFsm) regionID() uint64 {
	return pf.peer.regionId
}

func (pf *peerFsm) getPeer() *Peer {
	return pf.peer
}

func (pf *peerFsm) peerID() uint64 {
	return pf.peer.Peer.Id
}

func (pf *peerFsm) stop() {
	pf.stopped = true
}

func (pf *peerFsm) setPendingMergeState(state *rspb.MergeState) {
	pf.peer.PendingMergeState = state
}

func (pf *peerFsm) scheduleApplyingSnapshot() {
	pf.peer.Store().ScheduleApplyingSnapshot()
}

func (pf *peerFsm) hasPendingMergeApplyResult() bool {
	return pf.peer.PendingMergeApplyResult != nil
}

func (pf *peerFsm) isStopped() bool {
	return pf.stopped
}

/// Set a mailbox to Fsm, which should be used to send message to itself.
func (pf *peerFsm) setMailbox(mb *mailbox) {
	pf.mailbox = mb
}

/// Take the mailbox from Fsm. Implementation should ensure there will be
/// no reference to mailbox after calling this method.
func (pf *peerFsm) takeMailbox() *mailbox {
	mb := pf.mailbox
	pf.mailbox = nil
	return mb
}

type peerFsmDelegate struct {
	*peerFsm
	ctx *PollContext
}

func newPeerFsmDelegate(fsm *peerFsm, ctx *PollContext) *peerFsmDelegate {
	return &peerFsmDelegate{
		peerFsm: fsm,
		ctx:     ctx,
	}
}

func (d *peerFsmDelegate) tag() string {
	return d.peer.Tag
}

func (d *peerFsmDelegate) handleMsgs(msgs []Msg) {
	for _, msg := range msgs {
		switch msg.Type {
		case MsgTypeRaftMessage:
			raftMsg := msg.Data.(*rspb.RaftMessage)
			if err := d.onRaftMsg(raftMsg); err != nil {
				log.Errorf("%s handle raft message error %v", d.peer.Tag, err)
			}
		case MsgTypeRaftCmd:
			raftCMD := msg.Data.(*MsgRaftCmd)
			d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
		case MsgTypeTick:
			d.onTick()
		case MsgTypeApplyRes:
			res := msg.Data.(*applyTaskRes)
			if state := d.peer.PendingMergeApplyResult; state != nil {
				state.results = append(state.results, res)
				continue
			}
			d.onApplyResult(res)
		case MsgTypeSignificantMsg:
			d.onSignificantMsg(msg.Data.(*MsgSignificant))
		case MsgTypeSplitRegion:
			split := msg.Data.(*MsgSplitRegion)
			log.Infof("%s on split with %v", d.peer.Tag, split.SplitKeys)
			d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKeys, split.Callback)
		case MsgTypeComputeResult:
			result := msg.Data.(*MsgComputeHashResult)
			d.onHashComputed(result.Index, result.Hash)
		case MsgTypeRegionApproximateSize:
			d.onApproximateRegionSize(msg.Data.(uint64))
		case MsgTypeRegionApproximateKeys:
			d.onApproximateRegionKeys(msg.Data.(uint64))
		case MsgTypeCompactionDeclineBytes:
			d.onCompactionDeclinedBytes(msg.Data.(uint64))
		case MsgTypeHalfSplitRegion:
			half := msg.Data.(*MsgHalfSplitRegion)
			d.onScheduleHalfSplitRegion(half.RegionEpoch, half.Policy)
		case MsgTypeMergeResult:
			result := msg.Data.(*MsgMergeResult)
			d.onMergeResult(result.TargetPeer, result.Stale)
		case MsgTypeGcSnap:
			gcSnap := msg.Data.(*MsgGCSnap)
			d.onGCSnap(gcSnap.Snaps)
		case MsgTypeClearRegionSize:
			d.onClearRegionSize()
		case MsgTypeStart:
			d.start()
		case MsgTypeNoop:
		}
	}
}

func (d *peerFsmDelegate) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickPdHeartbeat) {
		d.onPDHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	if d.ticker.isOnTick(PeerTickCheckMerge) {
		d.onCheckMerge()
	}
	if d.ticker.isOnTick(PeerTickPeerStaleState) {
		d.onCheckPeerStaleStateTick()
	}
	d.ctx.tickDriverCh <- d.regionID()
}

func (d *peerFsmDelegate) start() {
	if d.peer.PendingMergeState != nil {
		d.notifyPrepareMerge()
	}
	d.ticker = newTicker(d.regionID(), d.ctx.cfg)
	d.ctx.tickDriverCh <- d.regionID()
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickPdHeartbeat)
	d.ticker.schedule(PeerTickPeerStaleState)
	d.onCheckMerge()
}

func (d *peerFsmDelegate) notifyPrepareMerge() {
	// TODO: merge func
}

func (d *peerFsmDelegate) resumeHandlePendingApplyResult() bool {
	return false // TODO: merge func
}

func (d *peerFsmDelegate) onGCSnap(snaps []SnapKeyWithSending) {
	store := d.peer.Store()
	compactedIdx := store.truncatedIndex()
	compactedTerm := store.truncatedTerm()
	isApplyingSnap := store.IsApplyingSnapshot()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.tag(), key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.tag(), key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > d.ctx.cfg.SnapGcTimeout {
					log.Infof("%s snap file %s has been expired, delete", d.tag(), key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || (key.Index == compactedIdx && !isApplyingSnap)) {
			log.Infof("%s snap file %s has been applied, delete", d.tag(), key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.tag(), key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerFsmDelegate) onClearRegionSize() {
	d.peer.ApproximateSize = nil
	d.peer.ApproximateKeys = nil
}

func (d *peerFsmDelegate) onSignificantMsg(msg *MsgSignificant) {
	switch msg.Type {
	case MsgSignificantTypeStatus:
		// Report snapshot status to the corresponding peer.
		d.reportSnapshotStatus(msg.ToPeerID, msg.SnapshotStatus)
	case MsgSignificantTypeUnreachable:
		d.peer.RaftGroup.ReportUnreachable(msg.ToPeerID)
	}
}

func (d *peerFsmDelegate) reportSnapshotStatus(toPeerID uint64, status raft.SnapshotStatus) {
	toPeer := d.peer.getPeerFromCache(toPeerID)
	if toPeer == nil {
		// If to_peer is gone, ignore this snapshot status
		log.Warnf("%s peer %d not found, ignore snapshot status %v", d.tag(), toPeerID, status)
		return
	}
	log.Infof("%s report snapshot status %s %v", d.tag(), toPeer, status)
	d.peer.RaftGroup.ReportSnapshot(toPeerID, status)
}

func (d *peerFsmDelegate) collectReady(proposals []*regionProposal) []*regionProposal {
	hasReady := d.hasReady
	d.hasReady = false
	if !hasReady || d.stopped {
		return proposals
	}
	d.ctx.pendingCount += 1
	d.ctx.hasReady = true
	if p := d.peer.TakeApplyProposals(); p != nil {
		proposals = append(proposals, p)
	}
	d.peer.HandleRaftReadyAppend(d.ctx)
	return proposals
}

func (d *peerFsmDelegate) postRaftReadyAppend(ready *raft.Ready, invokeCtx *InvokeContext) {
	isMerging := d.peer.PendingMergeState != nil
	res := d.peer.PostRaftReadyAppend(d.ctx, ready, invokeCtx)
	d.peer.HandleRaftReadyApply(d.ctx, ready)
	hasSnapshot := false
	if res != nil {
		d.onReadyApplySnapshot(res)
		hasSnapshot = true
	}
	if isMerging && hasSnapshot {
		// After applying a snapshot, merge is rollbacked implicitly.
		d.onReadyRollbackMerge(0, nil)
	}
}

func (d *peerFsmDelegate) regionID() uint64 {
	return d.peer.regionId
}

func (d *peerFsmDelegate) region() *metapb.Region {
	return d.peer.Store().region
}

func (d *peerFsmDelegate) storeID() uint64 {
	return d.peer.Peer.StoreId
}

func (d *peerFsmDelegate) onRaftBaseTick() {
	if d.peer.PendingRemove {
		return
	}
	// When having pending snapshot, if election timeout is met, it can't pass
	// the pending conf change check because first index has been updated to
	// a value that is larger than last index.
	if d.peer.IsApplyingSnapshot() || d.peer.HasPendingSnapshot() {
		// need to check if snapshot is applied.
		d.hasReady = true
		d.ticker.schedule(PeerTickRaft)
		return
	}
	// TODO: make Tick returns bool to indicate if there is ready.
	d.peer.RaftGroup.Tick()
	d.hasReady = d.peer.RaftGroup.HasReady()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerFsmDelegate) onApplyResult(res *applyTaskRes) {
	if res.destroyPeerID != 0 {
		y.Assert(res.destroyPeerID == d.peerID())
		d.destroyPeer(false)
	} else {
		log.Debugf("%s async apply finished %v", d.tag(), res)
		var readyToMerge *uint32
		readyToMerge, res.execResults = d.onReadyResult(res.merged, res.execResults)
		if readyToMerge != nil {
			// There is a `CommitMerge` needed to wait
			d.peer.PendingMergeApplyResult = &WaitApplyResultState{
				results:      []*applyTaskRes{res},
				readyToMerge: readyToMerge,
			}
			return
		}
		if d.stopped {
			return
		}
		if d.peer.PostApply(d.ctx, res.applyState, res.appliedIndexTerm, res.merged, res.metrics) {
			d.hasReady = true
		}
	}
}

func (d *peerFsmDelegate) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.tag(), msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.peer.PendingRemove || d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if msg.MergeTarget != nil {
		need, err := d.needGCMerge(msg)
		if err != nil {
			return err
		}
		if need {
			d.onStaleMerge()
		}
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.peer.insertPeerCache(msg.GetFromPeer())
	err = d.peer.Step(msg.GetMessage())
	if err != nil {
		return err
	}
	if d.peer.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.peer.HeartbeatPd(d.ctx)
	}
	d.hasReady = true
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerFsmDelegate) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerFsmDelegate) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := isVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
	// tell 2 is stale, so 2 can remove itself.
	region := d.peer.Region()
	if IsEpochStale(fromEpoch, region.RegionEpoch) && findPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		d.ctx.handleStaleMsg(msg, region.RegionEpoch, isVoteMsg, nil)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.peerID() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.tag(), target.Id, d.peerID())
		return true
	} else if target.Id > d.peerID() {
		if job := d.peer.MaybeDestroy(); job != nil {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.tag(), target)
			if d.handleDestroyPeer(job) {
				storeMsg := NewMsg(MsgTypeStoreRaftMessage, msg)
				d.ctx.router.sendControl(storeMsg)
			}
		}
		return true
	}
	return false
}

func (d *peerFsmDelegate) needGCMerge(msg *rspb.RaftMessage) (bool, error) {
	return false, nil // TODO: merge func
}

func (d *peerFsmDelegate) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !IsEpochStale(d.peer.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !PeerEqual(d.peer.Peer, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.tag())
		return
	}
	// TODO: ask pd to guarantee we are stale now.
	log.Infof("%s peer %s receives gc message, trying to remove", d.tag(), msg.ToPeer)
	if job := d.peer.MaybeDestroy(); job != nil {
		d.handleDestroyPeer(job)
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `SnapKey` is returned.
func (d *peerFsmDelegate) checkSnapshot(msg *rspb.RaftMessage) (*SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snap := msg.Message.Snapshot
	key := SnapKeyFromRegionSnap(regionID, snap)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.tag(), snapRegion, peerID)
		return &key, nil
	}
	var regionsToDestroy []uint64
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// destroy regions out of lock to avoid dead lock.
		destroyRegions(d.ctx.router, regionsToDestroy, d.getPeer().Peer)
	}()
	meta := d.ctx.storeMeta
	if !RegionEqual(meta.regions[d.regionID()], d.region()) {
		if !d.peer.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.tag())
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.tag(), meta.regions[d.regionID()], d.region()))
		}
	}
	for _, region := range meta.pendingSnapshotRegions {
		if bytes.Compare(region.StartKey, snapRegion.EndKey) < 0 &&
			bytes.Compare(region.EndKey, snapRegion.StartKey) > 0 &&
			// Same region can overlap, we will apply the latest version of snapshot.
			region.Id != snapRegion.Id {
			log.Infof("pending region overlapped regionID %d peerID %d region %s snap %s",
				d.regionID(), d.peerID(), region, snap)
			return &key, nil
		}
	}

	// In some extreme cases, it may cause source peer destroyed improperly so that a later
	// CommitMerge may panic because source is already destroyed, so just drop the message:
	// 1. A new snapshot is received whereas a snapshot is still in applying, and the snapshot
	// under applying is generated before merge and the new snapshot is generated after merge.
	// After the applying snapshot is finished, the log may able to catch up and so a
	// CommitMerge will be applied.
	// 2. There is a CommitMerge pending in apply thread.
	ready := !d.peer.IsApplyingSnapshot() && !d.peer.HasPendingSnapshot() && d.peer.ReadyToHandlePendingSnap()

	existRegions := d.findOverlapRegions(meta, snapRegion)
	for _, existRegion := range existRegions {
		log.Infof("%s region overlapped %s %s", d.tag(), existRegion, snapRegion)
		if ready && maybeDestroySource(meta, d.regionID(), existRegion.Id, snapRegion.RegionEpoch) {
			// The snapshot that we decide to whether destroy peer based on must can be applied.
			// So here not to destroy peer immediately, or the snapshot maybe dropped in later
			// check but the peer is already destroyed.
			regionsToDestroy = append(regionsToDestroy, existRegion.Id)
			continue
		}
		return &key, nil
	}
	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	meta.pendingSnapshotRegions = append(meta.pendingSnapshotRegions, snapRegion)
	d.ctx.queuedSnaps[regionID] = struct{}{}
	return nil, nil
}

func (d *peerFsmDelegate) findOverlapRegions(storeMeta *storeMeta, snapRegion *metapb.Region) (result []*metapb.Region) {
	it := storeMeta.regionRanges.NewIterator()
	it.Seek(snapRegion.StartKey)
	for it.Valid() {
		regionID := regionIDFromBytes(it.Value())
		if bytes.Equal(it.Key(), snapRegion.StartKey) || regionID == snapRegion.Id {
			it.Next()
			continue
		}
		region := storeMeta.regions[regionID]
		if bytes.Compare(region.StartKey, snapRegion.EndKey) < 0 {
			result = append(result, region)
		} else {
			return
		}
	}
	return
}

func (d *peerFsmDelegate) handleDestroyPeer(job *DestroyPeerJob) bool {
	if job.Initialized {
		d.ctx.applyRouter.scheduleTask(job.RegionId, NewPeerMsg(MsgTypeApplyDestroy, job.RegionId, nil))
	}
	if job.AsyncRemove {
		log.Infof("[region %d] %d is destroyed asynchronously", job.RegionId, job.Peer.Id)
		return false
	}
	d.destroyPeer(false)
	return true
}

func (d *peerFsmDelegate) destroyPeer(mergeByTarget bool) {
	log.Infof("%s starts destroy [merged_by_target: %v]", d.tag(), mergeByTarget)
	regionID := d.regionID()
	// We can't destroy a peer which is applying snapshot.
	y.Assert(!d.peer.IsApplyingSnapshot())
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// Send messages out of store meta lock.
		d.ctx.applyRouter.scheduleTask(regionID, NewPeerMsg(MsgTypeApplyDestroy, regionID, nil))
		// Trigger region change observer
		d.ctx.coprocessorHost.OnRegionChanged(d.region(), RegionChangeEvent_Destroy, d.peer.GetRole())
		d.ctx.pdScheduler <- task{
			tp: taskTypePDDestroyPeer,
			data: &pdDestroyPeerTask{
				regionID: regionID,
			},
		}
	}()
	meta := d.ctx.storeMeta
	delete(meta.pendingMergeTargets, regionID)
	if targetID, ok := meta.targetsMap[regionID]; ok {
		delete(meta.targetsMap, regionID)
		if target, ok1 := meta.pendingMergeTargets[targetID]; ok1 {
			delete(target, regionID)
			// When the target doesn't exist(add peer but the store is isolated), source peer decide to destroy by itself.
			// Without target, the `pending_merge_targets` for target won't be removed, so here source peer help target to clear.
			if meta.regions[targetID] == nil && len(meta.pendingMergeTargets[targetID]) == 0 {
				delete(meta.pendingMergeTargets, targetID)
			}
		}
	}
	delete(meta.mergeLocks, regionID)
	isInitialized := d.peer.isInitialized()
	if err := d.peer.Destroy(d.ctx, mergeByTarget); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.tag(), err))
	}
	d.ctx.router.close(regionID)
	d.stop()
	if isInitialized && !mergeByTarget && !meta.regionRanges.Delete(d.region().EndKey) {
		panic(d.tag() + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok && !mergeByTarget {
		panic(d.tag() + " meta corruption detected")
	}
	delete(meta.regions, regionID)
	d.ctx.peerEventObserver.OnPeerDestroy(d.peer.getEventContext())
}

func (d *peerFsmDelegate) onReadyChangePeer(cp changePeer) {
	changeType := cp.confChange.ChangeType
	d.peer.RaftGroup.ApplyConfChange(*cp.confChange)
	if cp.confChange.NodeId == 0 {
		// Apply failed, skip.
		return
	}
	d.ctx.storeMetaLock.Lock()
	d.ctx.storeMeta.setRegion(d.ctx.coprocessorHost, cp.region, d.peer)
	d.ctx.storeMetaLock.Unlock()
	peerID := cp.peer.Id
	switch changeType {
	case eraftpb.ConfChangeType_AddNode, eraftpb.ConfChangeType_AddLearnerNode:
		if d.peerID() == peerID && d.peer.Peer.IsLearner {
			d.peer.Peer = cp.peer
		}

		// Add this peer to cache and heartbeats.
		now := time.Now()
		d.peer.PeerHeartbeats[peerID] = now
		if d.peer.IsLeader() {
			d.peer.PeersStartPendingTime[peerID] = now
		}
		d.peer.RecentAddedPeer.Update(peerID, now)
		d.peer.insertPeerCache(cp.peer)
	case eraftpb.ConfChangeType_RemoveNode:
		// Remove this peer from cache.
		delete(d.peer.PeerHeartbeats, peerID)
		if d.peer.IsLeader() {
			delete(d.peer.PeersStartPendingTime, peerID)
		}
		d.peer.removePeerCache(peerID)
	}

	// In pattern matching above, if the peer is the leader,
	// it will push the change peer into `peers_start_pending_time`
	// without checking if it is duplicated. We move `heartbeat_pd` here
	// to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
	// adding the redundant peer.
	if d.peer.IsLeader() {
		// Notify pd immediately.
		log.Infof("%s notify pd with change peer region %s", d.tag(), d.region())
		d.peer.HeartbeatPd(d.ctx)
	}
	myPeerID := d.peerID()

	// We only care remove itself now.
	if changeType == eraftpb.ConfChangeType_RemoveNode && cp.peer.StoreId == d.storeID() {
		if myPeerID == peerID {
			d.destroyPeer(false)
		} else {
			panic(fmt.Sprintf("%s trying to remove unknown peer %s", d.tag(), cp.peer))
		}
	}
}

func (d *peerFsmDelegate) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
	totalCnt := d.peer.LastApplyingIdx - firstIndex
	// the size of current CompactLog command can be ignored.
	remainCnt := d.peer.LastApplyingIdx - truncatedIndex - 1
	d.peer.RaftLogSizeHint *= remainCnt / totalCnt
	raftLogGCTask := &raftLogGCTask{
		raftEngine: d.ctx.engine.raft,
		regionID:   d.regionID(),
		startIdx:   d.peer.LastCompactedIdx,
		endIdx:     truncatedIndex + 1,
	}
	d.peer.LastCompactedIdx = raftLogGCTask.endIdx
	d.peer.Store().CompactTo(raftLogGCTask.endIdx)
	d.ctx.raftLogGCScheduler <- task{
		tp:   taskTypeRaftLogGC,
		data: raftLogGCTask,
	}
}

func (d *peerFsmDelegate) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	regionID := derived.Id
	meta.setRegion(d.ctx.coprocessorHost, derived, d.getPeer())
	d.peer.PostSplit()
	isLeader := d.peer.IsLeader()
	if isLeader {
		d.peer.HeartbeatPd(d.ctx)
		// Notify pd immediately to let it update the region meta.
		log.Infof("%s notify pd with split count %d", d.tag(), len(regions))
		// Now pd only uses ReportBatchSplit for history operation show,
		// so we send it independently here.
		d.ctx.pdScheduler <- task{
			tp:   taskTypePDReportBatchSplit,
			data: &pdReportBatchSplitTask{regions: regions},
		}
	}

	lastRegion := regions[len(regions)-1]
	if !meta.regionRanges.Delete(lastRegion.EndKey) {
		panic(d.tag() + " original region should exist")
	}
	// It's not correct anymore, so set it to None to let split checker update it.
	d.peer.ApproximateSize = nil
	lastRegionID := lastRegion.Id

	newPeers := make([]*PeerEventContext, 0, len(regions))
	for _, newRegion := range regions {
		newRegionID := newRegion.Id
		notExist := meta.regionRanges.Insert(newRegion.EndKey, regionIDToBytes(newRegionID))
		y.Assert(notExist)
		if newRegionID == regionID {
			newPeers = append(newPeers, d.peer.getEventContext())
			continue
		}

		// Insert new regions and validation
		log.Infof("[region %d] inserts new region %s", regionID, newRegion)
		if r, ok := meta.regions[newRegionID]; ok {
			// Suppose a new node is added by conf change and the snapshot comes slowly.
			// Then, the region splits and the first vote message comes to the new node
			// before the old snapshot, which will create an uninitialized peer on the
			// store. After that, the old snapshot comes, followed with the last split
			// proposal. After it's applied, the uninitialized peer will be met.
			// We can remove this uninitialized peer directly.
			if len(r.Peers) > 0 {
				panic(fmt.Sprintf("[region %d] duplicated region %s for split region %s",
					newRegionID, r, newRegion))
			}
			d.ctx.router.close(newRegionID)
		}

		sender, newPeer, err := createPeerFsm(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionScheduler, d.ctx.engine, newRegion)
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
		}
		metaPeer := newPeer.peer.Peer
		newPeers = append(newPeers, newPeer.peer.getEventContext())

		for _, p := range newRegion.GetPeers() {
			newPeer.peer.insertPeerCache(p)
		}

		// New peer derive write flow from parent region,
		// this will be used by balance write flow.
		newPeer.peer.PeerStat = d.peer.PeerStat
		campaigned := newPeer.peer.MaybeCampaign(isLeader)
		newPeer.hasReady = newPeer.hasReady || campaigned

		if isLeader {
			// The new peer is likely to become leader, send a heartbeat immediately to reduce
			// client query miss.
			newPeer.peer.HeartbeatPd(d.ctx)
		}

		newPeer.peer.Activate(d.ctx)
		meta.regions[newRegionID] = newRegion
		if lastRegionID == newRegionID {
			// To prevent from big region, the right region needs run split
			// check again after split.
			newPeer.peer.SizeDiffHint = d.ctx.cfg.RegionSplitCheckDiff
		}
		mb := newMailbox(sender, newPeer)
		d.ctx.router.register(newRegionID, mb)
		_ = d.ctx.router.send(newRegionID, NewPeerMsg(MsgTypeStart, newRegionID, nil))
		if !campaigned {
			for i, msg := range meta.pendingVotes {
				if PeerEqual(msg.ToPeer, metaPeer) {
					meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
					_ = d.ctx.router.send(newRegionID, NewPeerMsg(MsgTypeRaftMessage, newRegionID, msg))
					break
				}
			}
		}
	}

	d.ctx.peerEventObserver.OnSplitRegion(derived, regions, newPeers)
}

func (d *peerFsmDelegate) validateMergePeer(targetRegion *metapb.Region) (bool, error) {
	return false, nil // TODO: merge func
}

func (d *peerFsmDelegate) scheduleMerge() error {
	return nil // TODO: merge func
}

func (d *peerFsmDelegate) rollbackMerge() {
	// TODO: merge func
}

func (d *peerFsmDelegate) onCheckMerge() {
	// TODO: merge func
}

func (d *peerFsmDelegate) onReadyPrepareMerge(region *metapb.Region, state *rspb.MergeState, merged bool) {
	// TODO: merge func
}

func (d *peerFsmDelegate) onReadyCommitMerge(region, source *metapb.Region) *uint32 {
	return nil // TODO: merge func
}

func (d *peerFsmDelegate) onReadyRollbackMerge(commit uint64, region *metapb.Region) {
	// TODO: merge func
}

func (d *peerFsmDelegate) onMergeResult(target *metapb.Peer, stale bool) {
	// TODO: merge func
}

func (d *peerFsmDelegate) onStaleMerge() {
	// TODO: merge func
}

func (d *peerFsmDelegate) onReadyApplySnapshot(applyResult *ApplySnapResult) {
	prevRegion := applyResult.PrevRegion
	region := applyResult.Region

	log.Infof("%s snapshot for region %s is applied", d.tag(), region)
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	initialized := len(prevRegion.Peers) > 0
	if initialized {
		log.Infof("%s region changed from %s -> %s after applying snapshot", d.tag(), prevRegion, region)
		meta.regionRanges.Delete(prevRegion.EndKey)
	}
	if !meta.regionRanges.Insert(region.EndKey, regionIDToBytes(region.Id)) {
		oldRegionID := regionIDFromBytes(meta.regionRanges.Get(region.EndKey, nil))
		panic(fmt.Sprintf("%s unexpected old region %d", d.tag(), oldRegionID))
	}
	meta.regions[region.Id] = region
	d.ctx.peerEventObserver.OnPeerApplySnap(d.peer.getEventContext(), region)
}

func (d *peerFsmDelegate) onReadyResult(merged bool, execResults []execResult) (*uint32, []execResult) {
	if len(execResults) == 0 {
		return nil, nil
	}

	// handle executing committed log results
	for i, result := range execResults {
		switch x := result.(type) {
		case *execResultChangePeer:
			d.onReadyChangePeer(x.cp)
		case *execResultCompactLog:
			if !merged {
				d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
			}
		case *execResultSplitRegion:
			d.onReadySplitRegion(x.derived, x.regions)
		case *execResultPrepareMerge:
			d.onReadyPrepareMerge(x.region, x.state, merged)
		case *execResultCommitMerge:
			if readyToMerge := d.onReadyCommitMerge(x.region, x.source); readyToMerge != nil {
				return readyToMerge, execResults[i:]
			}
		case *execResultRollbackMerge:
			d.onReadyRollbackMerge(x.commit, x.region)
		case *execResultComputeHash:
			d.onReadyComputeHash(x.region, x.index, x.snap)
		case *execResultVerifyHash:
			d.onReadyVerifyHash(x.index, x.hash)
		case *execResultDeleteRange:
			// TODO: clean user properties?
		}
	}
	return nil, nil
}

func (d *peerFsmDelegate) checkMergeProposal(msg *raft_cmdpb.RaftCmdRequest) error {
	return nil // TODO: merge func
}

func (d *peerFsmDelegate) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := checkStoreID(req, d.storeID()); err != nil {
		return nil, err
	}
	if req.StatusRequest != nil {
		// For status commands, we handle it here directly.
		return d.executeStatusCommand(req)
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionID()
	leaderID := d.peer.LeaderId()
	if !d.peer.IsLeader() {
		leader := d.peer.getPeerFromCache(leaderID)
		return nil, &ErrNotLeader{regionID, leader}
	}
	// peer_id must be the same as peer's.
	if err := checkPeerID(req, d.peerID()); err != nil {
		return nil, err
	}
	// Check whether the term is stale.
	if err := checkTerm(req, d.peer.Term()); err != nil {
		return nil, err
	}
	err := checkRegionEpoch(req, d.region(), true)
	if errEpochNotMatching, ok := err.(*ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return nil, errEpochNotMatching
	}
	return nil, err
}

func (d *peerFsmDelegate) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *Callback) {
	resp, err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if resp != nil {
		cb.Done(resp)
		return
	}

	if d.peer.PendingRemove {
		NotifyReqRegionRemoved(d.regionID(), cb)
		return
	}

	if err := d.checkMergeProposal(msg); err != nil {
		log.Warnf("%s failed to process merge, message %s, err %v", d.tag(), msg, err)
		cb.Done(ErrResp(err))
		return
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.

	resp = &raft_cmdpb.RaftCmdResponse{}
	BindRespTerm(resp, d.peer.Term())
	if d.peer.Propose(d.ctx, cb, msg, resp) {
		d.hasReady = true
	}

	// TODO: add timeout, if the command is not applied after timeout,
	// we will call the callback with timeout error.
}

func (d *peerFsmDelegate) findSiblingRegion() *metapb.Region {
	var start []byte
	var skipFirst bool
	if d.ctx.cfg.RightDeriveWhenSplit {
		start = d.region().StartKey
	} else {
		start = d.region().EndKey
		skipFirst = true
	}
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	it := meta.regionRanges.NewIterator()
	it.Seek(start)
	valid := it.Valid()
	if valid && skipFirst {
		it.Next()
		valid = it.Valid()
	}
	if !valid {
		return nil
	}
	regionID := regionIDFromBytes(it.Value())
	return meta.regions[regionID]
}

func (d *peerFsmDelegate) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)

	// As leader, we would not keep caches for the peers that didn't response heartbeat in the
	// last few seconds. That happens probably because another TiKV is down. In this case if we
	// do not clean up the cache, it may keep growing.
	dropCacheDuration := time.Duration(d.ctx.cfg.RaftHeartbeatTicks)*d.ctx.cfg.RaftBaseTickInterval +
		d.ctx.cfg.RaftEntryCacheLifeTime
	cacheAliveLimit := time.Now().Add(-dropCacheDuration)

	totalGCLogs := uint64(0)

	appliedIdx := d.peer.Store().AppliedIndex()
	if !d.peer.IsLeader() {
		d.peer.Store().CompactTo(appliedIdx + 1)
		return
	}

	// Leader will replicate the compact log command to followers,
	// If we use current replicated_index (like 10) as the compact index,
	// when we replicate this log, the newest replicated_index will be 11,
	// but we only compact the log to 10, not 11, at that time,
	// the first index is 10, and replicated_index is 11, with an extra log,
	// and we will do compact again with compact index 11, in cycles...
	// So we introduce a threshold, if replicated index - first index > threshold,
	// we will try to compact log.
	// raft log entries[..............................................]
	//                  ^                                       ^
	//                  |-----------------threshold------------ |
	//              first_index                         replicated_index
	// `alive_cache_idx` is the smallest `replicated_index` of healthy up nodes.
	// `alive_cache_idx` is only used to gc cache.
	truncatedIdx := d.peer.Store().truncatedIndex()
	lastIdx, _ := d.peer.Store().LastIndex()
	replicatedIdx, aliveCacheIdx := lastIdx, lastIdx
	for peerID, progress := range d.peer.RaftGroup.Raft.Prs {
		if replicatedIdx > progress.Match {
			replicatedIdx = progress.Match
		}
		if lastHeartbeat, ok := d.peer.PeerHeartbeats[peerID]; ok {
			if aliveCacheIdx > progress.Match &&
				progress.Match >= truncatedIdx &&
				lastHeartbeat.After(cacheAliveLimit) {
				aliveCacheIdx = progress.Match
			}
		}
	}
	// When an election happened or a new peer is added, replicated_idx can be 0.
	if replicatedIdx > 0 {
		y.Assert(lastIdx >= replicatedIdx)
	}
	d.peer.Store().MaybeGCCache(replicatedIdx, appliedIdx)
	firstIdx, _ := d.peer.Store().FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else if d.peer.RaftLogSizeHint >= d.ctx.cfg.RaftLogGcSizeLimit {
		compactIdx = appliedIdx
	} else if replicatedIdx < firstIdx || replicatedIdx-firstIdx <= d.ctx.cfg.RaftLogGcThreshold {
		return
	} else {
		compactIdx = replicatedIdx
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	totalGCLogs += compactIdx - firstIdx

	term, err := d.peer.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionID()
	request := newCompactLogRequest(regionID, d.peer.Peer, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerFsmDelegate) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	// TODO: check whether a gc progress has been started.
	if len(d.ctx.splitCheckScheduler) > 0 {
		return
	}

	if !d.peer.IsLeader() {
		return
	}

	// When restart, the approximate size will be None. The
	// split check will first check the region size, and then
	// check whether the region should split.  This should
	// work even if we change the region max size.
	// If peer says should update approximate size, update region
	// size and check whether the region should split.
	if d.peer.ApproximateSize != nil &&
		d.peer.CompactionDeclinedBytes < d.ctx.cfg.RegionSplitCheckDiff &&
		d.peer.SizeDiffHint < d.ctx.cfg.RegionSplitCheckDiff {
		return
	}
	d.ctx.splitCheckScheduler <- task{
		tp: taskTypeSplitCheck,
		data: &splitCheckTask{
			region:    d.region(),
			autoSplit: true,
			policy:    pdpb.CheckPolicy_SCAN,
		},
	}
	d.peer.SizeDiffHint = 0
	d.peer.CompactionDeclinedBytes = 0
}

func (d *peerFsmDelegate) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKeys [][]byte, cb *Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKeys); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.region()
	d.ctx.pdScheduler <- task{
		tp: taskTypePDAskBatchSplit,
		data: &pdAskBatchSplitTask{
			region:      region,
			splitKeys:   splitKeys,
			peer:        d.peer.Peer,
			rightDerive: d.ctx.cfg.RightDeriveWhenSplit,
			callback:    cb,
		},
	}
}

func (d *peerFsmDelegate) validateSplitRegion(epoch *metapb.RegionEpoch, splitKeys [][]byte) error {
	if len(splitKeys) == 0 {
		err := errors.Errorf("%s no split key is specified", d.tag())
		log.Error(err)
		return err
	}
	for _, key := range splitKeys {
		if len(key) == 0 {
			err := errors.Errorf("%s split key should not be empty", d.tag())
			log.Error(err)
			return err
		}
	}
	if !d.peer.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.tag())
		return &ErrNotLeader{
			RegionId: d.regionID(),
			Leader:   d.peer.getPeerFromCache(d.peer.LeaderId()),
		}
	}

	region := d.region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to PD.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.tag(), latestEpoch, epoch)
		return &ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.tag(), latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerFsmDelegate) onApproximateRegionSize(size uint64) {
	d.peer.ApproximateSize = &size
}

func (d *peerFsmDelegate) onApproximateRegionKeys(keys uint64) {
	d.peer.ApproximateKeys = &keys
}

func (d *peerFsmDelegate) onCompactionDeclinedBytes(declinedBytes uint64) {
	d.peer.CompactionDeclinedBytes += declinedBytes
}

func (d *peerFsmDelegate) onScheduleHalfSplitRegion(regionEpoch *metapb.RegionEpoch, policy pdpb.CheckPolicy) {
	if !d.peer.IsLeader() {
		log.Warnf("%s not leader, skip", d.tag())
		return
	}
	region := d.region()
	if IsEpochStale(regionEpoch, region.RegionEpoch) {
		log.Warnf("%s receive a stale halfsplit message", d.tag())
		return
	}
	d.ctx.splitCheckScheduler <- task{
		tp: taskTypeSplitCheck,
		data: &splitCheckTask{
			region: region,
			policy: policy,
		},
	}
}

func (d *peerFsmDelegate) onPDHeartbeatTick() {
	d.ticker.schedule(PeerTickPdHeartbeat)
	d.peer.CheckPeers()

	if !d.peer.IsLeader() {
		return
	}
	d.peer.HeartbeatPd(d.ctx)
}

func (d *peerFsmDelegate) onCheckPeerStaleStateTick() {
	if d.peer.PendingRemove {
		return
	}
	d.ticker.schedule(PeerTickPeerStaleState)

	if d.peer.IsApplyingSnapshot() || d.peer.HasPendingSnapshot() {
		return
	}

	// If this peer detects the leader is missing for a long long time,
	// it should consider itself as a stale peer which is removed from
	// the original cluster.
	// This most likely happens in the following scenario:
	// At first, there are three peer A, B, C in the cluster, and A is leader.
	// Peer B gets down. And then A adds D, E, F into the cluster.
	// Peer D becomes leader of the new cluster, and then removes peer A, B, C.
	// After all these peer in and out, now the cluster has peer D, E, F.
	// If peer B goes up at this moment, it still thinks it is one of the cluster
	// and has peers A, C. However, it could not reach A, C since they are removed
	// from the cluster or probably destroyed.
	// Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
	// In this case, peer B would notice that the leader is missing for a long time,
	// and it would check with pd to confirm whether it's still a member of the cluster.
	// If not, it destroys itself as a stale peer which is removed out already.
	state := d.peer.CheckStaleState(d.ctx)
	switch state {
	case StaleStateValid:
	case StaleStateLeaderMissing:
		log.Warnf("%s leader missing longer than abnormal_leader_missing_duration %v",
			d.tag(), d.ctx.cfg.AbnormalLeaderMissingDuration)
	case StaleStateToValidate:
		// for peer B in case 1 above
		log.Warnf("%s leader missing longer than max_leader_missing_duration %v. To check with pd whether it's still valid",
			d.tag(), d.ctx.cfg.AbnormalLeaderMissingDuration)
		d.ctx.pdScheduler <- task{
			tp: taskTypePDValidatePeer,
			data: &pdValidatePeerTask{
				region: d.region(),
				peer:   d.peer.Peer,
			},
		}
	}
}

func (d *peerFsmDelegate) onReadyComputeHash(region *metapb.Region, index uint64, snap *DBSnapshot) {
	d.peer.ConsistencyState.LastCheckTime = time.Now()
	log.Infof("%s schedule compute hash task", d.tag())
	d.ctx.computeHashScheduler <- task{
		tp: taskTypeComputeHash,
		data: &computeHashTask{
			region: region,
			index:  index,
			snap:   snap,
		},
	}
}

func (d *peerFsmDelegate) onReadyVerifyHash(expectedIndex uint64, expectedHash []byte) {
	d.verifyAndStoreHash(expectedIndex, expectedHash)
}

func (d *peerFsmDelegate) onHashComputed(index uint64, hash []byte) {
	if !d.verifyAndStoreHash(index, hash) {
		return
	}
	req := newVerifyHashRequest(d.regionID(), d.peer.Peer, d.peer.ConsistencyState)
	d.proposeRaftCommand(req, nil)
}

/// Verify and store the hash to state. return true means the hash has been stored successfully.
func (d *peerFsmDelegate) verifyAndStoreHash(expectedIndex uint64, expectedHash []byte) bool {
	state := d.peer.ConsistencyState
	index := state.Index
	if expectedIndex < index {
		log.Warnf("%s has scheduled a new hash, skip, index: %d, expected_index: %d, ",
			d.tag(), d.peer.ConsistencyState.Index, expectedIndex)
		return false
	}
	if expectedIndex == index {
		if len(state.Hash) == 0 {
			log.Warnf("%s duplicated consistency check detected, skip.", d.tag())
			return false
		}
		if !bytes.Equal(state.Hash, expectedHash) {
			panic(fmt.Sprintf("%s hash at %d not correct want %v, got %v",
				d.tag(), index, expectedHash, state.Hash))
		}
		log.Infof("%s consistency check pass, index %d", d.tag(), index)
		state.Hash = nil
		return false
	}
	if state.Index != 0 && len(state.Hash) > 0 {
		// Maybe computing is too slow or computed result is dropped due to channel full.
		// If computing is too slow, miss count will be increased twice.
		log.Warnf("%s hash belongs to wrong index, skip, index: %d, expected_index: %d",
			d.tag(), index, expectedIndex)
	}
	log.Infof("%s save hash for consistency check later, index: %d", d.tag(), index)
	state.Index = expectedIndex
	state.Hash = expectedHash
	return true
}

func maybeDestroySource(meta *storeMeta, targetID, sourceID uint64, epoch *metapb.RegionEpoch) bool {
	if mergeTargets, ok := meta.pendingMergeTargets[targetID]; ok {
		if targetEpoch, ok1 := mergeTargets[sourceID]; ok1 {
			log.Infof("[region %d] checking source %d epoch: %s, merge target epoch: %s",
				targetID, sourceID, epoch, targetEpoch)
			// The target peer will move on, namely, it will apply a snapshot generated after merge,
			// so destroy source peer.
			if epoch.Version > targetEpoch.Version {
				return true
			}
			// Wait till the target peer has caught up logs and source peer will be destroyed at that time.
			return false
		}
	}
	return false
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newVerifyHashRequest(regionID uint64, peer *metapb.Peer, state *ConsistencyState) *raft_cmdpb.RaftCmdRequest {
	request := newAdminRequest(regionID, peer)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_VerifyHash,
		VerifyHash: &raft_cmdpb.VerifyHashRequest{
			Index: state.Index,
			Hash:  state.Hash,
		},
	}
	return request
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

// Handle status commands here, separate the logic, maybe we can move it
// to another file later.
// Unlike other commands (write or admin), status commands only show current
// store status, so no need to handle it in raft group.
func (d *peerFsmDelegate) executeStatusCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	cmdType := request.StatusRequest.CmdType
	var response *raft_cmdpb.StatusResponse
	switch cmdType {
	case raft_cmdpb.StatusCmdType_RegionLeader:
		response = d.executeRegionLeader()
	case raft_cmdpb.StatusCmdType_RegionDetail:
		var err error
		response, err = d.executeRegionDetail(request)
		if err != nil {
			return nil, err
		}
	case raft_cmdpb.StatusCmdType_InvalidStatus:
		return nil, errors.New("invalid status command!")
	}
	response.CmdType = cmdType

	resp := &raft_cmdpb.RaftCmdResponse{
		StatusResponse: response,
	}
	BindRespTerm(resp, d.peer.Term())
	return resp, nil // TODO: stub
}

func (d *peerFsmDelegate) executeRegionLeader() *raft_cmdpb.StatusResponse {
	resp := &raft_cmdpb.StatusResponse{}
	if leader := d.peer.getPeerFromCache(d.peer.LeaderId()); leader != nil {
		resp.RegionLeader = &raft_cmdpb.RegionLeaderResponse{
			Leader: leader,
		}
	}
	return resp
}

func (d *peerFsmDelegate) executeRegionDetail(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.StatusResponse, error) {
	if !d.peer.isInitialized() {
		regionID := request.Header.RegionId
		return nil, errors.Errorf("region %d not initialized", regionID)
	}
	resp := &raft_cmdpb.StatusResponse{
		RegionDetail: &raft_cmdpb.RegionDetailResponse{
			Region: d.region(),
		},
	}
	if leader := d.peer.getPeerFromCache(d.peer.LeaderId()); leader != nil {
		resp.RegionDetail = &raft_cmdpb.RegionDetailResponse{
			Leader: leader,
		}
	}
	return resp, nil
}
