package raftstore

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

const (
	MaxSnapRetryCnt  = 5
	MaxCacheCapacity = 1024 - 1
)

// CompactRaftLog discards all log entries prior to compact_index. We must guarantee
// that the compact_index is not greater than applied index.
func CompactRaftLog(tag string, state *rspb.RaftApplyState, compactIndex, compactTerm uint64) error {
	log.Debugf("%s compact log entries to prior to %d", tag, compactIndex)

	if compactIndex <= state.TruncatedState.Index {
		return errors.New("try to truncate compacted entries")
	} else if compactIndex > state.AppliedIndex {
		return errors.Errorf("compact index %d > applied index %d", compactIndex, state.AppliedIndex)
	}

	// we don't actually delete the logs now, we add an async task to do it.
	state.TruncatedState.Index = compactIndex
	state.TruncatedState.Term = compactTerm
	return nil
}

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

type InvokeContext struct {
	RegionID   uint64
	RaftState  rspb.RaftLocalState
	ApplyState rspb.RaftApplyState
	lastTerm   uint64
	SnapRegion *metapb.Region
}

func NewInvokeContext(store *PeerStorage) *InvokeContext {
	ctx := &InvokeContext{
		RegionID:   store.region.GetId(),
		RaftState:  store.raftState,
		ApplyState: store.applyState,
		lastTerm:   store.lastTerm,
	}
	return ctx
}

func (ic *InvokeContext) hasSnapshot() bool {
	return ic.SnapRegion != nil
}

func (ic *InvokeContext) saveRaftStateTo(wb *engine_util.WriteBatch) {
	wb.SetMsg(meta.RaftStateKey(ic.RegionID), &ic.RaftState)
}

func (ic *InvokeContext) saveApplyStateTo(wb *engine_util.WriteBatch) {
	wb.SetMsg(meta.ApplyStateKey(ic.RegionID), &ic.ApplyState)
}

func (ic *InvokeContext) saveSnapshotRaftStateTo(snapshotIdx uint64, wb *engine_util.WriteBatch) {
	snapshotRaftState := ic.RaftState
	snapshotRaftState.HardState.Commit = snapshotIdx
	snapshotRaftState.LastIndex = snapshotIdx
	wb.SetMsg(meta.SnapshotRaftStateKey(ic.RegionID), &snapshotRaftState)
}

func recoverFromApplyingState(engines *engine_util.Engines, raftWB *engine_util.WriteBatch, regionID uint64) error {
	snapRaftState, err := meta.GetSnapRaftState(engines.Kv, regionID)
	if err != nil {
		return errors.Errorf("region %d failed to get raftstate from kv engine when recover from applying state", regionID)
	}

	raftState, err := meta.GetRaftLocalState(engines.Raft, regionID)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.WithStack(err)
	}

	// if we recv append log when applying snapshot, last_index in raft_local_state will
	// larger than snapshot_index. since raft_local_state is written to raft engine, and
	// raft write_batch is written after kv write_batch, raft_local_state may wrong if
	// restart happen between the two write. so we copy raft_local_state to kv engine
	// (snapshot_raft_state), and set snapshot_raft_state.last_index = snapshot_index.
	// after restart, we need check last_index.
	if snapRaftState.LastIndex > raftState.LastIndex {
		raftWB.SetMsg(meta.RaftStateKey(regionID), snapRaftState)
	}
	return nil
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	Engines *engine_util.Engines

	peerID           uint64
	region           *metapb.Region
	raftState        rspb.RaftLocalState
	applyState       rspb.RaftApplyState
	appliedIndexTerm uint64
	lastTerm         uint64

	snapState    snap.SnapState
	regionSched  chan<- worker.Task
	snapTriedCnt int

	Tag string
}

func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, peerID uint64, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	lastTerm, err := meta.InitLastTerm(engines.Raft, region, raftState, applyState)
	if err != nil {
		return nil, err
	}
	return &PeerStorage{
		Engines:     engines,
		peerID:      peerID,
		region:      region,
		Tag:         tag,
		raftState:   *raftState,
		applyState:  *applyState,
		lastTerm:    lastTerm,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raftState.HardState.Commit == 0 && raftState.HardState.Term == 0 && raftState.HardState.Vote == 0 {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) IsApplyingSnapshot() bool {
	return ps.snapState.StateType == snap.SnapState_Applying
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}
	ents := make([]eraftpb.Entry, 0, high-low)
	if low == high {
		return ents, nil
	}
	ents, _, err = fetchEntriesTo(ps.Engines.Raft, ps.region.Id, low, high, ents)
	if err != nil {
		return ents, err
	}
	return ents, nil
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.lastTerm || idx == ps.raftState.LastIndex {
		return ps.lastTerm, nil
	}
	entries, err := ps.Entries(idx, idx+1)
	if err != nil {
		return 0, err
	}
	return entries[0].Term, nil
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return firstIndex(&ps.applyState), nil
}

func firstIndex(applyState *rspb.RaftApplyState) uint64 {
	return applyState.TruncatedState.Index + 1
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("snapshot is stale, generate again, regionID: %d, peerID: %d, snapIndex: %d, truncatedIndex: %d", ps.region.GetId(), ps.peerID, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("failed to decode snapshot, it may be corrupted, regionID: %d, peerID: %d, err: %v", ps.region.GetId(), ps.peerID, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("snapshot epoch is stale, regionID: %d, peerID: %d, snapEpoch: %s, latestEpoch: %s", ps.region.GetId(), ps.peerID, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			snapshot = *s
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("failed to try generating snapshot, regionID: %d, peerID: %d, times: %d", ps.region.GetId(), ps.peerID, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= MaxSnapRetryCnt {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("requesting snapshot, regionID: %d, peerID: %d", ps.region.GetId(), ps.peerID)
	ps.snapTriedCnt++
	ps.ScheduleGenerateSnapshot()

	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) ScheduleGenerateSnapshot() {
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	ps.regionSched <- worker.Task{
		Tp: worker.TaskTypeRegionGen,
		Data: &runner.RegionTask{
			RegionId: ps.region.GetId(),
			Notifier: ch,
		},
	}
}

// Append the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *PeerStorage) Append(invokeCtx *InvokeContext, entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	log.Debugf("%s append %d entries", ps.Tag, len(entries))
	prevLastIndex := invokeCtx.RaftState.LastIndex
	if len(entries) == 0 {
		return nil
	}
	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.Index
	lastTerm := lastEntry.Term
	for _, entry := range entries {
		err := raftWB.SetMsg(meta.RaftLogKey(ps.region.Id, entry.Index), &entry)
		if err != nil {
			return err
		}
	}
	// Delete any previously appended log entries which never committed.
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		raftWB.Delete(meta.RaftLogKey(ps.region.Id, i))
	}
	invokeCtx.RaftState.LastIndex = lastIndex
	invokeCtx.lastTerm = lastTerm
	return nil
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	regionId := newRegion.Id
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.regionSched <- worker.Task{
			Tp: worker.TaskTypeRegionDestroy,
			Data: &runner.RegionTask{
				RegionId: regionId,
				StartKey: oldStartKey,
				EndKey:   newStartKey,
			},
		}
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.regionSched <- worker.Task{
			Tp: worker.TaskTypeRegionDestroy,
			Data: &runner.RegionTask{
				RegionId: regionId,
				StartKey: newEndKey,
				EndKey:   oldEndKey,
			},
		}
	}
}

func fetchEntriesTo(engine *badger.DB, regionID, low, high uint64, buf []eraftpb.Entry) ([]eraftpb.Entry, uint64, error) {
	var totalSize uint64
	nextIndex := low
	txn := engine.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(regionID, low)
	endKey := meta.RaftLogKey(regionID, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		var entry eraftpb.Entry
		err = entry.Unmarshal(val)
		if err != nil {
			return nil, 0, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		totalSize += uint64(len(val))
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, totalSize, nil
	}
	// Here means we don't fetch enough entries.
	return nil, 0, raft.ErrUnavailable
}

func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.Delete(meta.RegionStateKey(regionID))
	kvWB.Delete(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.Delete(meta.RaftLogKey(regionID, i))
	}
	raftWB.Delete(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

func WritePeerState(kvWB *engine_util.WriteBatch, region *metapb.Region, state rspb.PeerState) {
	regionID := region.Id
	regionState := new(rspb.RegionLocalState)
	regionState.State = state
	regionState.Region = region
	data, _ := regionState.Marshal()
	kvWB.Set(meta.RegionStateKey(regionID), data)
}

// Apply the peer with given snapshot.
func (ps *PeerStorage) ApplySnapshot(ctx *InvokeContext, snap *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) error {
	log.Infof("%v begin to apply snapshot", ps.Tag)

	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snap.Data); err != nil {
		return err
	}

	if snapData.Region.Id != ps.region.Id {
		return fmt.Errorf("mismatch region id %v != %v", snapData.Region.Id, ps.region.Id)
	}

	if ps.isInitialized() {
		// we can only delete the old data when the peer is initialized.
		if err := ps.clearMeta(kvWB, raftWB); err != nil {
			return err
		}
	}

	WritePeerState(kvWB, snapData.Region, rspb.PeerState_Applying)

	lastIdx := snap.Metadata.Index

	ctx.RaftState.LastIndex = lastIdx
	ctx.lastTerm = snap.Metadata.Term
	ctx.ApplyState.AppliedIndex = lastIdx

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.ApplyState.TruncatedState.Index = lastIdx
	ctx.ApplyState.TruncatedState.Term = snap.Metadata.Term

	log.Debugf("%v apply snapshot for region %v with state %v ok", ps.Tag, snapData.Region, ctx.ApplyState)

	ctx.SnapRegion = snapData.Region
	return nil
}

/// Save memory states to disk.
///
/// This function only write data to `ready_ctx`'s `WriteBatch`. It's caller's duty to write
/// it explicitly to disk. If it's flushed to disk successfully, `post_ready` should be called
/// to update the memory states properly.
/// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(kvWB, raftWB *engine_util.WriteBatch, ready *raft.Ready) (*InvokeContext, error) {
	ctx := NewInvokeContext(ps)
	var snapshotIdx uint64 = 0
	if !raft.IsEmptySnap(&ready.Snapshot) {
		if err := ps.ApplySnapshot(ctx, &ready.Snapshot, kvWB, raftWB); err != nil {
			return nil, err
		}
		snapshotIdx = ctx.RaftState.LastIndex
	}

	if len(ready.Entries) != 0 {
		if err := ps.Append(ctx, ready.Entries, raftWB); err != nil {
			return nil, err
		}
	}

	// Last index is 0 means the peer is created from raft message
	// and has not applied snapshot yet, so skip persistent hard state.
	if ctx.RaftState.LastIndex > 0 {
		if !raft.IsEmptyHardState(ready.HardState) {
			ctx.RaftState.HardState = &ready.HardState
		}
	}

	if !proto.Equal(&ctx.RaftState, &ps.raftState) {
		ctx.saveRaftStateTo(raftWB)
		if snapshotIdx > 0 {
			// in case of restart happen when we just write region state to Applying,
			// but not write raft_local_state to raft rocksdb in time.
			// we write raft state to default rocksdb, with last index set to snap index,
			// in case of recv raft log after snapshot.
			ctx.saveSnapshotRaftStateTo(snapshotIdx, kvWB)
		}
	}

	// only when apply snapshot
	if !proto.Equal(&ctx.ApplyState, &ps.applyState) {
		ctx.saveApplyStateTo(kvWB)
	}

	return ctx, nil
}

// Update the memory state after ready changes are flushed to disk successfully.
func (ps *PeerStorage) PostReadyPersistent(ctx *InvokeContext) *ApplySnapResult {
	ps.raftState = ctx.RaftState
	ps.applyState = ctx.ApplyState
	ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.SnapRegion == nil {
		return nil
	}
	// cleanup data before scheduling apply worker.Task
	if ps.isInitialized() {
		ps.clearExtraData(ps.region)
	}

	ps.ScheduleApplyingSnapshot()
	prevRegion := ps.region
	ps.region = ctx.SnapRegion
	ctx.SnapRegion = nil

	return &ApplySnapResult{
		PrevRegion: prevRegion,
		Region:     ps.region,
	}
}

func (ps *PeerStorage) ScheduleApplyingSnapshot() {
	status := snap.JobStatus_Pending
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
		Status:    &status,
	}
	ps.regionSched <- worker.Task{
		Tp: worker.TaskTypeRegionApply,
		Data: &runner.RegionTask{
			RegionId: ps.region.Id,
			Status:   &status,
		},
	}
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) ClearData() error {
	ps.regionSched <- worker.Task{
		Tp: worker.TaskTypeRegionDestroy,
		Data: &runner.RegionTask{
			RegionId: ps.region.GetId(),
			StartKey: ps.region.GetStartKey(),
			EndKey:   ps.region.GetEndKey(),
		},
	}
	return nil
}

func (p *PeerStorage) CancelApplyingSnap() bool {
	// Todo: currently it is a place holder
	return true
}

// Check if the storage is applying a snapshot.
func (p *PeerStorage) CheckApplyingSnap() bool {
	switch p.snapState.StateType {
	case snap.SnapState_Applying:
		switch atomic.LoadUint32(p.snapState.Status) {
		case snap.JobStatus_Finished:
			p.snapState = snap.SnapState{StateType: snap.SnapState_Relax}
		case snap.JobStatus_Cancelled:
			p.snapState = snap.SnapState{StateType: snap.SnapState_ApplyAborted}
		case snap.JobStatus_Failed:
			panic(fmt.Sprintf("%v applying snapshot failed", p.Tag))
		default:
			return true
		}
	}
	return false
}
