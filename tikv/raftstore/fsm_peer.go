package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
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
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeerFsm(storeID uint64, cfg *Config, sched chan<- *RegionTask,
	engines *Engines, region *metapb.Region) (chan<- Msg, *peerFsm, error) {
	metaPeer := findPeer(region, storeID)
	if metaPeer == nil {
		return nil, nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	metaPeer = ClonePeer(metaPeer)
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	ch := make(chan Msg, msgDefaultChanSize)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, nil, err
	}
	return (chan<- Msg)(ch), &peerFsm{
		peer:     peer,
		receiver: ch,
	}, nil
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeerFsm(storeID uint64, cfg *Config, sched chan<- *RegionTask,
	engines *Engines, regionID uint64, metaPeer *metapb.Peer) (chan<- Msg, *peerFsm, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id: regionID,
	}
	ch := make(chan Msg, msgDefaultChanSize)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, nil, err
	}
	return (chan<- Msg)(ch), &peerFsm{
		peer:     peer,
		receiver: ch,
	}, nil
}

func (pf *peerFsm) drop() {
	pf.peer.Stop()
	for {
		select {
		case msg := <-pf.receiver:
			var cb Callback
			switch msg.Type {
			case MsgTypeRaftCmd:
				cb = msg.Data.(*MsgRaftCmd).Callback
			case MsgTypeSplitRegion:
				cb = msg.Data.(*MsgSplitRegion).Callback
			default:
				continue
			}
			cb(ErrRespRegionNotFound(pf.regionID()), nil)
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
	ctx    *PollContext
	ticker *ticker
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
			res := msg.Data.(*ApplyTaskRes)
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
			d.onApprocximateRegionKeys(msg.Data.(uint64))
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
	d.ticker.step()
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
}

func (d *peerFsmDelegate) start() {
	if d.peer.PendingMergeState != nil {
		d.notifyPrepareMerge()
	}
	d.ticker = newTicker(d.regionID(), d.ctx.Cfg)
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
				if time.Since(modTime) > d.ctx.Cfg.SnapGcTimeout {
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
	toPeer := d.peer.GetPeerFromCache(toPeerID)
	if toPeer == nil {
		// If to_peer is gone, ignore this snapshot status
		log.Warnf("%s peer %d not found, ignore snapshot status %v", d.tag(), toPeerID, status)
		return
	}
	log.Infof("%s report snapshot status %s %v", d.tag(), toPeer, status)
	d.peer.RaftGroup.ReportSnapshot(toPeerID, status)
}

func (d *peerFsmDelegate) collectReady(proposals []*RegionProposal) []*RegionProposal {
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

func (d *peerFsmDelegate) onApplyResult(res *ApplyTaskRes) {
	if res.destroyPeerID != 0 {
		y.Assert(res.destroyPeerID == d.peerID())
		d.destroyPeer(false)
	} else {
		log.Debugf("%s async apply finished %v", d.tag(), res)
		if readyToMerge := d.onReadyResult(res.merged, res.execResults); readyToMerge != nil {
			// There is a `CommitMerge` needed to wait
			d.peer.PendingMergeApplyResult = &WaitApplyResultState{
				results:      []*ApplyTaskRes{res},
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
	d.peer.InsertPeerCache(msg.FromPeer)
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
				storeMsg := Msg{Type: MsgTypeRaftMessage, Data: msg}
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

	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	if !RegionEqual(meta.regions[d.regionID()], d.region()) {
		if !d.peer.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.tag())
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.tag(), meta.regions[d.regionID()], d.region()))
		}
	}
	existOverlapRegion := d.findOverlapRegion(meta, snapRegion)
	if existOverlapRegion != nil {
		log.Infof("%s region overlapped %s %s", d.tag(), existOverlapRegion, snapRegion)
		// In some extreme case, it may happen that a new snapshot is received whereas a snapshot is still in applying
		// if the snapshot under applying is generated before merge and the new snapshot is generated after merge,
		// update `pending_cross_snap` here may cause source peer destroys itself improperly. So don't update
		// `pending_cross_snap` here if peer is applying snapshot.
		if !d.peer.IsApplyingSnapshot() && !d.peer.HasPendingSnapshot() {
			meta.pendingCrossSnap[regionID] = snapRegion.RegionEpoch
		}
		return &key, nil
	}
	for _, region := range meta.pendingSnapshotRegions {
		if bytes.Compare(region.StartKey, snapRegion.EndKey) < 0 &&
			bytes.Compare(region.EndKey, snapRegion.StartKey) > 0 &&
			// Same region can overlap, we will apply the latest version of snapshot.
			region.Id != snapRegion.Id {
			log.Infof("%s pending region overlapped %s, %s", d.tag(), region, snapRegion)
			return &key, nil
		}
	}
	if r, ok := meta.pendingCrossSnap[regionID]; ok {
		if IsEpochStale(snapRegion.RegionEpoch, r) {
			log.Infof("%s snapshot epoch is stale %s, %s, drop", d.tag(), snapRegion.RegionEpoch, r)
			return &key, nil
		}
	}
	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	meta.pendingSnapshotRegions = append(meta.pendingSnapshotRegions, snapRegion)
	d.ctx.queuedSnaps[regionID] = struct{}{}
	delete(meta.pendingCrossSnap, regionID)
	return nil, nil
}

func (d *peerFsmDelegate) findOverlapRegion(storeMeta *storeMeta, snapRegion *metapb.Region) *metapb.Region {
	it := storeMeta.regionRanges.NewIterator()
	it.Seek(snapRegion.StartKey)
	for it.Valid() {
		regionID := binary.LittleEndian.Uint64(it.Value())
		if bytes.Equal(it.Key(), snapRegion.StartKey) || regionID == snapRegion.Id {
			it.Next()
			continue
		}
		region := storeMeta.regions[regionID]
		if bytes.Compare(region.StartKey, snapRegion.EndKey) < 0 {
			return region
		} else {
			return nil
		}
	}
	return nil
}

func (d *peerFsmDelegate) handleDestroyPeer(job *DestroyPeerJob) bool {
	return false // TODO: stub
}

func (d *peerFsmDelegate) destroyPeer(mergeTarget bool) {
	// TODO: stub
}

func (d *peerFsmDelegate) onReadyChangePeer(cp *pdpb.ChangePeer) {
	// TODO: stub
}

func (d *peerFsmDelegate) onReadyCompactLog(firstIndex uint64, state *rspb.RaftTruncatedState) {
	// TODO: stub
}

func (d *peerFsmDelegate) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	// TODO: stub
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
	// TODO: stub
}

func (d *peerFsmDelegate) onReadyResult(merged bool, execResults []execResult) *uint32 {
	return nil // TODO: stub
}

func (d *peerFsmDelegate) checkMergeProposal(msg *raft_cmdpb.RaftCmdRequest) error {
	return nil // TODO: merge func
}

func (d *peerFsmDelegate) preProposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	return nil, nil // TODO: stub
}

func (d *peerFsmDelegate) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb Callback) {
	// TODO: stub
}

func (d *peerFsmDelegate) findSiblingRegion() *metapb.Region {
	return nil // TODO: stub
}

func (d *peerFsmDelegate) onRaftGCLogTick() {
	// TODO: stub
}

func (d *peerFsmDelegate) onSplitRegionCheckTick() {
	// TODO: stub
}

func (d *peerFsmDelegate) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKeys [][]byte, cb Callback) {
	// TODO: stub
}

func (d *peerFsmDelegate) validateSplitRegion(regionEpoch *metapb.RegionEpoch, splitKeys [][]byte) error {
	return nil // TODO: stub
}

func (d *peerFsmDelegate) onApproximateRegionSize(size uint64) {
	// TODO: stub
}

func (d *peerFsmDelegate) onApprocximateRegionKeys(keys uint64) {
	// TODO: stub
}

func (d *peerFsmDelegate) onCompactionDeclinedBytes(declinedBytes uint64) {
	// TODO: stub
}

func (d *peerFsmDelegate) onScheduleHalfSplitRegion(epoch *metapb.RegionEpoch, policy pdpb.CheckPolicy) {
	// TODO: stub
}

func (d *peerFsmDelegate) onPDHeartbeatTick() {
	// TODO: stub
}

func (d *peerFsmDelegate) onCheckPeerStaleStateTick() {
	// TODO: stub
}

func (d *peerFsmDelegate) onReadyComputeHash(region *metapb.Region, index uint64, snap *DBSnapshot) {
	// TODO: stub
}

func (d *peerFsmDelegate) onReadyVerifyHash(expectedIndex uint64, expectedHash []byte) {
	// TODO: stub
}

func (d *peerFsmDelegate) onHashComputed(index uint64, hash []byte) {
	// TODO: stub
}

func (d *peerFsmDelegate) onIngestSSTResult(ssts []import_sstpb.SSTMeta) {
	// TODO: stub
}

func (d *peerFsmDelegate) verifyAndStoreHash(expectedIndex uint64, expectedHash []byte) bool {
	return false // TODO: stub
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return nil // TODO: stub
}

func newVerifyHashRequest(regionID uint64, peer *metapb.Peer, state *ConsistencyState) *raft_cmdpb.RaftCmdRequest {
	return nil // TODO: stub
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	return nil // TODO: stub
}

func (d *peerFsmDelegate) executeStatusCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	return nil, nil // TODO: stub
}

func (d *peerFsmDelegate) executeRegionLeader() (*raft_cmdpb.StatusResponse, error) {
	return nil, nil // TODO: stub
}

func (d *peerFsmDelegate) executeRegionDetail() (*raft_cmdpb.StatusResponse, error) {
	return nil, nil // TODO: stub
}
