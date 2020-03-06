package runner

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

// Region Task represents a task related to region for region-worker.
// There're some tasks, such as
// `TaskTypeRegionGen` which will cause the worker to generate a snapshot according to RegionId,
// `TaskTypeRegionApply` which will apply a snapshot to the region that id equals RegionId,
// `TaskTypeRegionDestroy` which will clean up the key range from StartKey to EndKey.
// When region-worker receive a Task, it would handle it according to task type. The type is not in RegionTask but in Task.Tp. RegionTask is the data field of Task.
type RegionTask struct {
	RegionId uint64                   // specify the region which the task is for.
	Notifier chan<- *eraftpb.Snapshot // useful in `TaskTypeRegionGen`, when it finishes snapshot generating, it notifies notifier.
	Status   *snap.JobStatus          // useful in `TaskTypeRegionApply` to see if the job is canceled
	StartKey []byte
	EndKey   []byte // `StartKey` and `EndKey` are useful in `TaskTypeRegionDestroy` to destroy certain range of region.
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
	task := t.Data.(*RegionTask)
	switch t.Tp {
	case worker.TaskTypeRegionGen:
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		r.ctx.handleGen(task.RegionId, task.Notifier)
	case worker.TaskTypeRegionApply:
		r.ctx.handleApply(task.RegionId, task.Status)
	case worker.TaskTypeRegionDestroy:
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

// cleanUpOriginData clear up the region data before applying snapshot
func (snapCtx *snapContext) cleanUpOriginData(regionState *rspb.RegionLocalState, status *snap.JobStatus) error {
	startKey, endKey := regionState.GetRegion().StartKey, regionState.GetRegion().EndKey
	if err := snap.CheckAbort(status); err != nil {
		return err
	}
	if err := engine_util.DeleteRange(snapCtx.engines.Kv, startKey, endKey); err != nil {
		return err
	}
	if err := snap.CheckAbort(status); err != nil {
		return err
	}
	return nil
}

// applySnap applies snapshot data of the Region.
func (snapCtx *snapContext) applySnap(regionId uint64, status *snap.JobStatus) error {
	log.Infof("begin apply snap data. [regionId: %d]", regionId)
	if err := snap.CheckAbort(status); err != nil {
		return err
	}

	regionKey := meta.RegionStateKey(regionId)
	regionState, err := meta.GetRegionLocalState(snapCtx.engines.Kv, regionId)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get regionState from %v", regionKey))
	}

	// Clean up origin data
	if err := snapCtx.cleanUpOriginData(regionState, status); err != nil {
		return err
	}

	applyState, err := meta.GetApplyState(snapCtx.engines.Kv, regionId)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get raftState from %v", meta.ApplyStateKey(regionId)))
	}
	snapKey := snap.SnapKey{RegionID: regionId, Index: applyState.TruncatedState.Index, Term: applyState.TruncatedState.Term}
	snapCtx.mgr.Register(snapKey, snap.SnapEntryApplying)
	defer snapCtx.mgr.Deregister(snapKey, snap.SnapEntryApplying)

	snapshot, err := snapCtx.mgr.GetSnapshotForApplying(snapKey)
	if err != nil {
		return errors.New(fmt.Sprintf("missing snapshot file %s", snapshot.Path()))
	}

	t := time.Now()
	applyOptions := snap.NewApplyOptions(snapCtx.engines.Kv, regionState.GetRegion(), status)
	if err := snapshot.Apply(*applyOptions); err != nil {
		return err
	}

	regionState.State = rspb.PeerState_Normal
	wb := new(engine_util.WriteBatch)
	wb.SetMsg(meta.RegionStateKey(regionId), regionState)
	wb.Delete(meta.SnapshotRaftStateKey(regionId))
	if err := wb.WriteToDB(snapCtx.engines.Kv); err != nil {
		log.Errorf("update region status failed: %s", err)
	}

	log.Infof("applying new data. [regionId: %d, timeTakes: %v]", regionId, time.Now().Sub(t))
	return nil
}

// handleApply tries to apply the snapshot of the specified Region. It calls `applySnap` to do the actual work.
func (snapCtx *snapContext) handleApply(regionId uint64, status *snap.JobStatus) {
	atomic.CompareAndSwapUint32(status, snap.JobStatus_Pending, snap.JobStatus_Running)
	err := snapCtx.applySnap(regionId, status)
	switch err.(type) {
	case nil:
		atomic.SwapUint32(status, snap.JobStatus_Finished)
	case snap.ApplySnapAbortError:
		log.Warnf("applying snapshot is aborted. [regionId: %d]", regionId)
		y.Assert(atomic.SwapUint32(status, snap.JobStatus_Cancelled) == snap.JobStatus_Cancelling)
	default:
		log.Errorf("failed to apply snap!!!. err: %v", err)
		atomic.SwapUint32(status, snap.JobStatus_Failed)
	}
}

// cleanUpRange cleans up the data within the range.
func (snapCtx *snapContext) cleanUpRange(regionId uint64, startKey, endKey []byte) {
	if err := engine_util.DeleteRange(snapCtx.engines.Kv, startKey, endKey); err != nil {
		log.Errorf("failed to delete data in range, [regionId: %d, startKey: %s, endKey: %s, err: %v]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey), err)
	} else {
		log.Infof("succeed in deleting data in range. [regionId: %d, startKey: %s, endKey: %s]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}
}

func getAppliedIdxTermForSnapshot(raft *badger.DB, kv *badger.Txn, regionId uint64) (uint64, uint64, error) {
	applyState := new(rspb.RaftApplyState)
	val, err := engine_util.GetValueTxn(kv, meta.ApplyStateKey(regionId))
	if err != nil {
		return 0, 0, err
	}
	applyState.Unmarshal(val)

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
	val, err := engine_util.GetValueTxn(txn, meta.RegionStateKey(regionId))
	if err != nil {
		return nil, err
	}
	err = regionState.Unmarshal(val)
	if err != nil {
		return nil, err
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
