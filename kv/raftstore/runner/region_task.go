package runner

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

// There're some tasks for region worker, such as:
// `TaskTypeRegionGen` which will cause the worker to generate a snapshot according to RegionId,
// `TaskTypeRegionApply` which will apply a snapshot to the region that id equals RegionId,
// `TaskTypeRegionDestroy` which will clean up the key range from StartKey to EndKey.

type RegionTaskGen struct {
	RegionId uint64                   // specify the region which the task is for.
	Notifier chan<- *eraftpb.Snapshot // when it finishes snapshot generating, it notifies notifier.
}

type RegionTaskApply struct {
	RegionId uint64                    // specify the region which the task is for.
	Notifier chan<- bool               // when it finishes snapshot applying, it notifies notifier.
	SnapMeta *eraftpb.SnapshotMetadata // the region meta information of the snapshot
	StartKey []byte                    // `StartKey` and `EndKey` are origin region's range, it's used to clean up certain range of region before applying snapshot.
	EndKey   []byte
}

type RegionTaskDestroy struct {
	RegionId uint64 // specify the region which the task is for.
	StartKey []byte // `StartKey` and `EndKey` are used to destroy certain range of region.
	EndKey   []byte
}

type regionTaskHandler struct {
	ctx *snapContext
}

func NewRegionTaskHandler(engines *engine_util.Engines, mgr *snap.SnapManager) *regionTaskHandler {
	return &regionTaskHandler{
		ctx: &snapContext{
			engines: engines,
			mgr:     mgr,
		},
	}
}

func (r *regionTaskHandler) Handle(t worker.Task) {
	switch t.(type) {
	case *RegionTaskGen:
		task := t.(*RegionTaskGen)
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		r.ctx.handleGen(task.RegionId, task.Notifier)
	case *RegionTaskApply:
		task := t.(*RegionTaskApply)
		r.ctx.handleApply(task.RegionId, task.Notifier, task.StartKey, task.EndKey, task.SnapMeta)
	case *RegionTaskDestroy:
		task := t.(*RegionTaskDestroy)
		r.ctx.cleanUpRange(task.RegionId, task.StartKey, task.EndKey)
	}
}

type snapContext struct {
	engines   *engine_util.Engines
	batchSize uint64
	mgr       *snap.SnapManager
}

// handleGen handles the task of generating snapshot of the Region.
func (snapCtx *snapContext) handleGen(regionId uint64, notifier chan<- *eraftpb.Snapshot) {
	snap, err := doSnapshot(snapCtx.engines, snapCtx.mgr, regionId)
	if err != nil {
		log.Errorf("failed to generate snapshot!!!, [regionId: %d, err : %v]", regionId, err)
	} else {
		notifier <- snap
	}
}

// applySnap applies snapshot data of the Region.
func (snapCtx *snapContext) applySnap(regionId uint64, startKey, endKey []byte, snapMeta *eraftpb.SnapshotMetadata) error {
	log.Infof("begin apply snap data. [regionId: %d]", regionId)

	// cleanUpOriginData clear up the region data before applying snapshot
	snapCtx.cleanUpRange(regionId, startKey, endKey)

	snapKey := snap.SnapKey{RegionID: regionId, Index: snapMeta.Index, Term: snapMeta.Term}
	snapCtx.mgr.Register(snapKey, snap.SnapEntryApplying)
	defer snapCtx.mgr.Deregister(snapKey, snap.SnapEntryApplying)

	snapshot, err := snapCtx.mgr.GetSnapshotForApplying(snapKey)
	if err != nil {
		return errors.New(fmt.Sprintf("missing snapshot file %s", err))
	}

	t := time.Now()
	applyOptions := snap.NewApplyOptions(snapCtx.engines.Kv, &metapb.Region{
		Id:       regionId,
		StartKey: startKey,
		EndKey:   endKey,
	})
	if err := snapshot.Apply(*applyOptions); err != nil {
		return err
	}

	log.Infof("applying new data. [regionId: %d, timeTakes: %v]", regionId, time.Now().Sub(t))
	return nil
}

// handleApply tries to apply the snapshot of the specified Region. It calls `applySnap` to do the actual work.
func (snapCtx *snapContext) handleApply(regionId uint64, notifier chan<- bool, startKey, endKey []byte, snapMeta *eraftpb.SnapshotMetadata) {
	err := snapCtx.applySnap(regionId, startKey, endKey, snapMeta)
	if err != nil {
		notifier <- false
		log.Fatalf("failed to apply snap!!!. err: %v", err)
	}
	notifier <- true
}

// cleanUpRange cleans up the data within the range.
func (snapCtx *snapContext) cleanUpRange(regionId uint64, startKey, endKey []byte) {
	if err := engine_util.DeleteRange(snapCtx.engines.Kv, startKey, endKey); err != nil {
		log.Fatalf("failed to delete data in range, [regionId: %d, startKey: %s, endKey: %s, err: %v]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey), err)
	} else {
		log.Infof("succeed in deleting data in range. [regionId: %d, startKey: %s, endKey: %s]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}
}

func getAppliedIdxTermForSnapshot(raft *badger.DB, kv *badger.Txn, regionId uint64) (uint64, uint64, error) {
	applyState := new(rspb.RaftApplyState)
	err := engine_util.GetMetaFromTxn(kv, meta.ApplyStateKey(regionId), applyState)
	if err != nil {
		return 0, 0, err
	}

	idx := applyState.AppliedIndex
	var term uint64
	if idx == applyState.TruncatedState.Index {
		term = applyState.TruncatedState.Term
	} else {
		entry, err := meta.GetRaftEntry(raft, regionId, idx)
		if err != nil {
			return 0, 0, err
		} else {
			term = entry.GetTerm()
		}
	}
	return idx, term, nil
}

func doSnapshot(engines *engine_util.Engines, mgr *snap.SnapManager, regionId uint64) (*eraftpb.Snapshot, error) {
	log.Debugf("begin to generate a snapshot. [regionId: %d]", regionId)

	txn := engines.Kv.NewTransaction(false)

	index, term, err := getAppliedIdxTermForSnapshot(engines.Raft, txn, regionId)
	if err != nil {
		return nil, err
	}

	key := snap.SnapKey{RegionID: regionId, Index: index, Term: term}
	mgr.Register(key, snap.SnapEntryGenerating)
	defer mgr.Deregister(key, snap.SnapEntryGenerating)

	regionState := new(rspb.RegionLocalState)
	err = engine_util.GetMetaFromTxn(txn, meta.RegionStateKey(regionId), regionState)
	if err != nil {
		panic(err)
	}
	if regionState.GetState() != rspb.PeerState_Normal {
		return nil, errors.Errorf("snap job %d seems stale, skip", regionId)
	}

	region := regionState.GetRegion()
	confState := util.ConfStateFromRegion(region)
	snapshot := &eraftpb.Snapshot{
		Metadata: &eraftpb.SnapshotMetadata{
			Index:     key.Index,
			Term:      key.Term,
			ConfState: &confState,
		},
	}
	s, err := mgr.GetSnapshotForBuilding(key)
	if err != nil {
		return nil, err
	}
	// Set snapshot data
	snapshotData := &rspb.RaftSnapshotData{Region: region}
	snapshotStatics := snap.SnapStatistics{}
	err = s.Build(txn, region, snapshotData, &snapshotStatics, mgr)
	if err != nil {
		return nil, err
	}
	snapshot.Data, err = snapshotData.Marshal()
	return snapshot, err
}
