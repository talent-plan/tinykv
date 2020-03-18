package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	Engines *engine_util.Engines

	peerID    uint64
	region    *metapb.Region
	raftState rspb.RaftLocalState

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
	return &PeerStorage{
		Engines:     engines,
		peerID:      peerID,
		region:      region,
		Tag:         tag,
		raftState:   *raftState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
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

	if ps.snapTriedCnt >= 5 {
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
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
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
	return ps.applyState().TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState().TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState().AppliedIndex
}

func (ps *PeerStorage) applyState() *rspb.RaftApplyState {
	state, _ := meta.GetApplyState(ps.Engines.Kv, ps.region.GetId())
	return state
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

// Append the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	log.Debugf("%s append %d entries", ps.Tag, len(entries))
	prevLastIndex := ps.raftState.LastIndex
	if len(entries) == 0 {
		return nil
	}
	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.Index
	lastTerm := lastEntry.Term
	for _, entry := range entries {
		err := raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, entry.Index), &entry)
		if err != nil {
			return err
		}
	}
	// Delete any previously appended log entries which never committed.
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, i))
	}
	ps.raftState.LastIndex = lastIndex
	ps.raftState.LastTerm = lastTerm
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
		ps.regionSched <- &runner.RegionTaskDestroy{
			RegionId: regionId,
			StartKey: oldStartKey,
			EndKey:   newStartKey,
		}
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.regionSched <- &runner.RegionTaskDestroy{
			RegionId: regionId,
			StartKey: newEndKey,
			EndKey:   oldEndKey,
		}
	}
}

func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

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
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Apply the peer with given snapshot.
func (ps *PeerStorage) ApplySnapshot(snap *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)

	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snap.Data); err != nil {
		return nil, err
	}

	if snapData.Region.Id != ps.region.Id {
		return nil, fmt.Errorf("mismatch region id %v != %v", snapData.Region.Id, ps.region.Id)
	}

	if ps.isInitialized() {
		// we can only delete the old data when the peer is initialized.
		if err := ps.clearMeta(kvWB, raftWB); err != nil {
			return nil, err
		}
	}

	ps.raftState.LastIndex = snap.Metadata.Index
	ps.raftState.LastTerm = snap.Metadata.Term

	applyRes := &ApplySnapResult{
		PrevRegion: ps.region,
		Region:     snapData.Region,
	}
	// cleanup data before scheduling apply worker.Task
	if ps.isInitialized() {
		ps.clearExtraData(snapData.Region)
	}
	ps.region = snapData.Region
	applyState := &rspb.RaftApplyState{
		AppliedIndex: snap.Metadata.Index,
		// The snapshot only contains log which index > applied index, so
		// here the truncate state's (index, term) is in snapshot metadata.
		TruncatedState: &rspb.RaftTruncatedState{
			Index: snap.Metadata.Index,
			Term:  snap.Metadata.Term,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.GetId()), applyState)
	ps.ScheduleApplyingSnapshotAndWait(snapData.Region, snap.Metadata)
	meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)

	log.Debugf("%v apply snapshot for region %v with state %v ok", ps.Tag, snapData.Region, applyState)
	return applyRes, nil
}

/// Save memory states to disk.
///
/// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	kvWB, raftWB := new(engine_util.WriteBatch), new(engine_util.WriteBatch)
	prevRaftState := ps.raftState

	var applyRes *ApplySnapResult = nil
	var err error
	if !raft.IsEmptySnap(&ready.Snapshot) {
		applyRes, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
	}

	if len(ready.Entries) != 0 {
		if err := ps.Append(ready.Entries, raftWB); err != nil {
			return nil, err
		}
	}

	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}

	if !proto.Equal(&prevRaftState, &ps.raftState) {
		raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), &ps.raftState)
	}

	kvWB.MustWriteToDB(ps.Engines.Kv)
	raftWB.MustWriteToDB(ps.Engines.Raft)
	return applyRes, nil
}

func (ps *PeerStorage) ScheduleApplyingSnapshotAndWait(snapRegion *metapb.Region, snapMeta *eraftpb.SnapshotMetadata) {
	ch := make(chan bool)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
	}
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapMeta,
		StartKey: snapRegion.GetStartKey(),
		EndKey:   snapRegion.GetEndKey(),
	}
	<-ch
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) ClearData() {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: ps.region.GetId(),
		StartKey: ps.region.GetStartKey(),
		EndKey:   ps.region.GetEndKey(),
	}
}
