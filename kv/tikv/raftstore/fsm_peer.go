package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/tablecodec"
)

type PeerTick int

const (
	PeerTickRaft             PeerTick = 0
	PeerTickRaftLogGC        PeerTick = 1
	PeerTickSplitRegionCheck PeerTick = 2
	PeerTickPdHeartbeat      PeerTick = 3
)

type peerFsm struct {
	peer     *Peer
	stopped  bool
	hasReady bool
	ticker   *ticker
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeerFsm(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peerFsm, error) {
	metaPeer := findPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, err
	}
	return &peerFsm{
		peer:   peer,
		ticker: newTicker(region.GetId(), cfg),
	}, nil
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeerFsm(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peerFsm, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, err
	}
	return &peerFsm{
		peer:   peer,
		ticker: newTicker(region.GetId(), cfg),
	}, nil
}

func (pf *peerFsm) drop() {
	pf.peer.Stop()
}

func (pf *peerFsm) regionID() uint64 {
	return pf.peer.regionId
}

func (pf *peerFsm) region() *metapb.Region {
	return pf.peer.Store().region
}

func (pf *peerFsm) getPeer() *Peer {
	return pf.peer
}

func (pf *peerFsm) storeID() uint64 {
	return pf.peer.Meta.StoreId
}

func (pf *peerFsm) peerID() uint64 {
	return pf.peer.Meta.Id
}

func (pf *peerFsm) stop() {
	pf.stopped = true
}

func (pf *peerFsm) scheduleApplyingSnapshot() {
	pf.peer.Store().ScheduleApplyingSnapshot()
}

func (pf *peerFsm) tag() string {
	return pf.peer.Tag
}

type peerMsgHandler struct {
	*peerFsm
	ctx *RaftContext
}

func newRaftMsgHandler(fsm *peerFsm, ctx *RaftContext) *peerMsgHandler {
	return &peerMsgHandler{
		peerFsm: fsm,
		ctx:     ctx,
	}
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	// It's an encoded key.
	// TODO: support meta key.
	SplitKeys [][]byte
	Callback  *message.Callback
}

type MsgGCSnap struct {
	Snaps []snap.SnapKeyWithSending
}

func (d *peerMsgHandler) HandleMsgs(msgs ...message.Msg) {
	for _, msg := range msgs {
		switch msg.Type {
		case message.MsgTypeRaftMessage:
			raftMsg := msg.Data.(*rspb.RaftMessage)
			if err := d.onRaftMsg(raftMsg); err != nil {
				log.Errorf("%s handle raft message error %v", d.peer.Tag, err)
			}
		case message.MsgTypeRaftCmd:
			raftCMD := msg.Data.(*message.MsgRaftCmd)
			d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
		case message.MsgTypeTick:
			d.onTick()
		case message.MsgTypeApplyRes:
			res := msg.Data.(*applyTaskRes)
			d.onApplyResult(res)
		case message.MsgTypeSignificantMsg:
			d.onSignificantMsg(msg.Data.(*MsgSignificant))
		case message.MsgTypeSplitRegion:
			split := msg.Data.(*MsgSplitRegion)
			log.Infof("%s on split with %v", d.peer.Tag, split.SplitKeys)
			d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKeys, split.Callback)
		// TODO: should update approximate size in split checker
		case message.MsgTypeRegionApproximateSize:
			d.onApproximateRegionSize(msg.Data.(uint64))
		case message.MsgTypeGcSnap:
			gcSnap := msg.Data.(*MsgGCSnap)
			d.onGCSnap(gcSnap.Snaps)
		case message.MsgTypeStart:
			d.startTicker()
		case message.MsgTypeNoop:
		}
	}
}

func (d *peerMsgHandler) onTick() {
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
	d.ctx.tickDriverSender <- d.regionID()
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionID(), d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionID()
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickPdHeartbeat)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
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

func (d *peerMsgHandler) onClearRegionSize() {
	d.peer.ApproximateSize = nil
}

func (d *peerMsgHandler) onSignificantMsg(msg *MsgSignificant) {
	switch msg.Type {
	case MsgSignificantTypeStatus:
		// Report snapshot status to the corresponding peer.
		d.reportSnapshotStatus(msg.ToPeerID, msg.SnapshotStatus)
	case MsgSignificantTypeUnreachable:
		d.peer.RaftGroup.ReportUnreachable(msg.ToPeerID)
	}
}

func (d *peerMsgHandler) reportSnapshotStatus(toPeerID uint64, status raft.SnapshotStatus) {
	toPeer := d.peer.getPeerFromCache(toPeerID)
	if toPeer == nil {
		// If to_peer is gone, ignore this snapshot status
		log.Warnf("%s peer %d not found, ignore snapshot status %v", d.tag(), toPeerID, status)
		return
	}
	log.Infof("%s report snapshot status %s %v", d.tag(), toPeer, status)
	d.peer.RaftGroup.ReportSnapshot(toPeerID, status)
}

func (d *peerMsgHandler) HandleRaftReadyAppend(proposals []*regionProposal) []*regionProposal {
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
	readyRes := d.peer.HandleRaftReadyAppend(d.ctx.trans, d.ctx.applyMsgs, d.ctx.kvWB, d.ctx.raftWB)
	if readyRes != nil {
		d.ctx.ReadyRes = append(d.ctx.ReadyRes, readyRes)
		ss := readyRes.Ready.SoftState
		if ss != nil && ss.RaftState == raft.StateLeader {
			d.peer.HeartbeatPd(d.ctx.pdTaskSender)
		}
	}
	return proposals
}

func (d *peerMsgHandler) PostRaftReadyPersistent(ready *raft.Ready, invokeCtx *InvokeContext) {
	res := d.peer.PostRaftReadyPersistent(d.ctx.trans, d.ctx.applyMsgs, ready, invokeCtx)
	d.peer.HandleRaftReadyApply(d.ctx.engine.Kv, d.ctx.applyMsgs, ready)
	if res != nil {
		d.onReadyApplySnapshot(res)
	}
}

func (d *peerMsgHandler) onRaftBaseTick() {
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

func (d *peerMsgHandler) onApplyResult(res *applyTaskRes) {
	if res.destroyPeerID != 0 {
		y.Assert(res.destroyPeerID == d.peerID())
		d.destroyPeer(false)
	} else {
		log.Debugf("%s async apply finished %v", d.tag(), res)
		res.execResults = d.onReadyResult(res.execResults)
		if d.stopped {
			return
		}
		if d.peer.PostApply(d.ctx.engine.Kv, res.applyState, res.appliedIndexTerm, res.sizeDiffHint) {
			d.hasReady = true
		}
	}
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
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
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
	}
	d.hasReady = true
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
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
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
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
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
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
				storeMsg := message.NewMsg(message.MsgTypeStoreRaftMessage, msg)
				d.ctx.router.sendStore(storeMsg)
			}
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !IsEpochStale(d.peer.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !PeerEqual(d.peer.Meta, msg.ToPeer) {
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
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
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
	defer func() {
		d.ctx.storeMetaLock.Unlock()
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

	existRegions := d.findOverlapRegions(meta, snapRegion)
	for _, existRegion := range existRegions {
		log.Infof("%s region overlapped %s %s", d.tag(), existRegion, snapRegion)
		return &key, nil
	}

	for _, region := range meta.pendingSnapshotRegions {
		if bytes.Compare(region.StartKey, snapRegion.EndKey) < 0 &&
			bytes.Compare(region.EndKey, snapRegion.StartKey) > 0 &&
			// Same region can overlap, we will apply the latest version of snapshot.
			region.Id != snapRegion.Id {
			log.Infof("pending region overlapped regionID %d peerID %d region %s snap %s",
				d.regionID(), d.peerID(), region, snapshot)
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
	return nil, nil
}

func (d *peerMsgHandler) findOverlapRegions(storeMeta *storeMeta, snapRegion *metapb.Region) (result []*metapb.Region) {
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

func (d *peerMsgHandler) handleDestroyPeer(job *DestroyPeerJob) bool {
	if job.Initialized {
		d.ctx.applyMsgs.appendMsg(job.RegionId, message.NewPeerMsg(message.MsgTypeApplyDestroy, job.RegionId, nil))
	}
	if job.AsyncRemove {
		log.Infof("[region %d] %d is destroyed asynchronously", job.RegionId, job.Peer.Id)
		return false
	}
	d.destroyPeer(false)
	return true
}

func (d *peerMsgHandler) destroyPeer(mergeByTarget bool) {
	log.Infof("%s starts destroy [merged_by_target: %v]", d.tag(), mergeByTarget)
	regionID := d.regionID()
	// We can't destroy a peer which is applying snapshot.
	y.Assert(!d.peer.IsApplyingSnapshot())
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// send messages out of store meta lock.
		d.ctx.applyMsgs.appendMsg(regionID, message.NewPeerMsg(message.MsgTypeApplyDestroy, regionID, nil))
		d.ctx.pdTaskSender <- worker.Task{
			Tp: worker.TaskTypePDDestroyPeer,
			Data: &pdDestroyPeerTask{
				regionID: regionID,
			},
		}
	}()
	meta := d.ctx.storeMeta
	isInitialized := d.peer.isInitialized()
	if err := d.peer.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.tag(), err))
	}
	d.ctx.router.close(regionID)
	d.stop()
	if isInitialized && !meta.regionRanges.Delete(d.region().EndKey) {
		panic(d.tag() + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.tag() + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) onReadyChangePeer(cp changePeer) {
	changeType := cp.confChange.ChangeType
	d.peer.RaftGroup.ApplyConfChange(*cp.confChange)
	if cp.confChange.NodeId == 0 {
		// Apply failed, skip.
		return
	}
	d.ctx.storeMetaLock.Lock()
	d.ctx.storeMeta.setRegion(cp.region, d.peer)
	d.ctx.storeMetaLock.Unlock()
	peerID := cp.peer.Id
	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		// Add this peer to cache and heartbeats.
		now := time.Now()
		d.peer.PeerHeartbeats[peerID] = now
		if d.peer.IsLeader() {
			d.peer.PeersStartPendingTime[peerID] = now
		}
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
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
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

func (d *peerMsgHandler) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
	totalCnt := d.peer.LastApplyingIdx - firstIndex
	// the size of current CompactLog command can be ignored.
	remainCnt := d.peer.LastApplyingIdx - truncatedIndex - 1
	d.peer.RaftLogSizeHint *= remainCnt / totalCnt
	raftLogGCTask := &raftLogGCTask{
		raftEngine: d.ctx.engine.Raft,
		regionID:   d.regionID(),
		startIdx:   d.peer.LastCompactedIdx,
		endIdx:     truncatedIndex + 1,
	}
	d.peer.LastCompactedIdx = raftLogGCTask.endIdx
	d.peer.Store().CompactTo(raftLogGCTask.endIdx)
	d.ctx.raftLogGCTaskSender <- worker.Task{
		Tp:   worker.TaskTypeRaftLogGC,
		Data: raftLogGCTask,
	}
}

func (d *peerMsgHandler) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	regionID := derived.Id
	meta.setRegion(derived, d.getPeer())
	d.peer.PostSplit()
	isLeader := d.peer.IsLeader()
	if isLeader {
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
		// Notify pd immediately to let it update the region meta.
		log.Infof("%s notify pd with split count %d", d.tag(), len(regions))
		// Now pd only uses ReportBatchSplit for history operation show,
		// so we send it independently here.
		d.ctx.pdTaskSender <- worker.Task{
			Tp:   worker.TaskTypePDReportBatchSplit,
			Data: &pdReportBatchSplitTask{regions: regions},
		}
	}

	lastRegion := regions[len(regions)-1]
	if !meta.regionRanges.Delete(lastRegion.EndKey) {
		panic(d.tag() + " original region should exist")
	}
	// It's not correct anymore, so set it to None to let split checker update it.
	d.peer.ApproximateSize = nil
	lastRegionID := lastRegion.Id

	for _, newRegion := range regions {
		newRegionID := newRegion.Id
		notExist := meta.regionRanges.Insert(newRegion.EndKey, regionIDToBytes(newRegionID))
		y.Assert(notExist)
		if newRegionID == regionID {
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

		newPeer, err := createPeerFsm(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
		}
		metaPeer := newPeer.peer.Meta

		for _, p := range newRegion.GetPeers() {
			newPeer.peer.insertPeerCache(p)
		}

		// New peer derive write flow from parent region,
		// this will be used by balance write flow.
		campaigned := newPeer.peer.MaybeCampaign(isLeader)
		newPeer.hasReady = newPeer.hasReady || campaigned

		if isLeader {
			// The new peer is likely to become leader, send a heartbeat immediately to reduce
			// client query miss.
			newPeer.peer.HeartbeatPd(d.ctx.pdTaskSender)
		}

		newPeer.peer.Activate(d.ctx.applyMsgs)
		meta.regions[newRegionID] = newRegion
		if lastRegionID == newRegionID {
			// To prevent from big region, the right region needs run split
			// check again after split.
			newPeer.peer.SizeDiffHint = d.ctx.cfg.RegionSplitCheckDiff
		}
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeStart, newRegionID, nil))
		if !campaigned {
			for i, msg := range meta.pendingVotes {
				if PeerEqual(msg.ToPeer, metaPeer) {
					meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
					_ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeRaftMessage, newRegionID, msg))
					break
				}
			}
		}
	}
}

func (d *peerMsgHandler) onReadyApplySnapshot(applyResult *ApplySnapResult) {
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
}

func (d *peerMsgHandler) onReadyResult(execResults []execResult) []execResult {
	if len(execResults) == 0 {
		return nil
	}

	// handle executing committed log results
	for _, result := range execResults {
		switch x := result.(type) {
		case *execResultChangePeer:
			d.onReadyChangePeer(x.cp)
		case *execResultCompactLog:
			d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
		case *execResultSplitRegion:
			d.onReadySplitRegion(x.derived, x.regions)
		}
	}
	return nil
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := checkStoreID(req, d.storeID()); err != nil {
		return nil, err
	}
	// only for test
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

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
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

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.

	resp = &raft_cmdpb.RaftCmdResponse{}
	BindRespTerm(resp, d.peer.Term())
	if d.peer.Propose(d.ctx.engine.Kv, d.ctx.cfg, cb, msg, resp) {
		d.hasReady = true
	}

	// TODO: add timeout, if the command is not applied after timeout,
	// we will call the callback with timeout error.
}

func (d *peerMsgHandler) findSiblingRegion() *metapb.Region {
	var start []byte
	start = d.region().StartKey
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	it := meta.regionRanges.NewIterator()
	it.Seek(start)
	if !it.Valid() {
		return nil
	}
	regionID := regionIDFromBytes(it.Value())
	return meta.regions[regionID]
}

func (d *peerMsgHandler) onRaftGCLogTick() {
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
	prs := d.peer.RaftGroup.Status().Progress
	for peerID, progress := range prs {
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
	request := newCompactLogRequest(regionID, d.peer.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.peer.IsLeader() {
		return
	}
	if d.peer.SizeDiffHint < d.ctx.cfg.RegionSplitCheckDiff {
		return
	}
	d.ctx.splitCheckTaskSender <- worker.Task{
		Tp: worker.TaskTypeSplitCheck,
		Data: &splitCheckTask{
			region: d.region(),
		},
	}
	d.peer.SizeDiffHint = 0
}

func isTableKey(key []byte) bool {
	return bytes.HasPrefix(key, tablecodec.TablePrefix())
}

func isSameTable(leftKey, rightKey []byte) bool {
	return bytes.HasPrefix(leftKey, tablecodec.TablePrefix()) &&
		bytes.HasPrefix(rightKey, tablecodec.TablePrefix()) &&
		len(leftKey) >= tablecodec.TableSplitKeyLen &&
		len(rightKey) >= tablecodec.TableSplitKeyLen &&
		bytes.Compare(leftKey[:tablecodec.TableSplitKeyLen], rightKey[:tablecodec.TableSplitKeyLen]) == 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKeys [][]byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKeys); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.region()
	d.ctx.pdTaskSender <- worker.Task{
		Tp: worker.TaskTypePDAskBatchSplit,
		Data: &pdAskBatchSplitTask{
			region:    region,
			splitKeys: splitKeys,
			peer:      d.peer.Meta,
			callback:  cb,
		},
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKeys [][]byte) error {
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

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.peer.ApproximateSize = &size
}

func (d *peerMsgHandler) onPDHeartbeatTick() {
	d.ticker.schedule(PeerTickPdHeartbeat)
	d.peer.CheckPeers()

	if !d.peer.IsLeader() {
		return
	}
	d.peer.HeartbeatPd(d.ctx.pdTaskSender)
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
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
func (d *peerMsgHandler) executeStatusCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
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

func (d *peerMsgHandler) executeRegionLeader() *raft_cmdpb.StatusResponse {
	resp := &raft_cmdpb.StatusResponse{}
	if leader := d.peer.getPeerFromCache(d.peer.LeaderId()); leader != nil {
		resp.RegionLeader = &raft_cmdpb.RegionLeaderResponse{
			Leader: leader,
		}
	}
	return resp
}

func (d *peerMsgHandler) executeRegionDetail(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.StatusResponse, error) {
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
