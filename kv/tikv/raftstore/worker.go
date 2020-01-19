package raftstore

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

type regionTask struct {
	regionId uint64
	notifier chan<- *eraftpb.Snapshot
	status   *JobStatus
	startKey []byte
	endKey   []byte
}

type raftLogGCTask struct {
	raftEngine *badger.DB
	regionID   uint64
	startIdx   uint64
	endIdx     uint64
}

type splitCheckTask struct {
	region *metapb.Region
}

type pdAskBatchSplitTask struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	callback  *message.Callback
}

type pdRegionHeartbeatTask struct {
	region          *metapb.Region
	peer            *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	approximateSize *uint64
}

type pdStoreHeartbeatTask struct {
	stats    *pdpb.StoreStats
	engine   *badger.DB
	path     string
	capacity uint64
}

type pdReportBatchSplitTask struct {
	regions []*metapb.Region
}

type pdDestroyPeerTask struct {
	regionID uint64
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  *router
	config  *config.SplitCheckConfig
	checker *sizeSplitChecker
}

func newSplitCheckHandler(engine *badger.DB, router *router, config *config.SplitCheckConfig) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		config:  config,
		checker: newSizeSplitChecker(config.RegionMaxSize, config.RegionSplitSize, config.BatchSplitLimit),
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	spCheckTask := t.Data.(*splitCheckTask)
	region := spCheckTask.region
	regionId := region.Id
	_, startKey, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		log.Errorf("failed to decode region key %x, err:%v", region.StartKey, err)
		return
	}
	_, endKey, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		log.Errorf("failed to decode region key %x, err:%v", region.EndKey, err)
		return
	}
	log.Debugf("executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	keys := r.splitCheck(startKey, endKey)
	if len(keys) != 0 {
		regionEpoch := region.GetRegionEpoch()
		for i, k := range keys {
			keys[i] = codec.EncodeBytes(nil, k)
		}
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &MsgSplitRegion{
				RegionEpoch: regionEpoch,
				SplitKeys:   keys,
			},
		}
		err = r.router.send(regionId, msg)
		if err != nil {
			log.Warnf("failed to send check result: [regionId: %d, err: %v]", regionId, err)
		}
	} else {
		log.Debugf("no need to send, split key not found: [regionId: %v]", regionId)
	}
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(startKey, endKey []byte) [][]byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	it := engine_util.NewCFIterator(engine_util.CF_DEFAULT, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		if r.checker.onKv(key, item) {
			break
		}
	}
	keys := r.checker.getSplitKeys()
	if len(keys) > 0 {
		return keys
	}
	return nil
}

type sizeSplitChecker struct {
	maxSize         uint64
	splitSize       uint64
	currentSize     uint64
	splitKeys       [][]byte
	batchSplitLimit uint64
}

func newSizeSplitChecker(maxSize, splitSize, batchSplitLimit uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:         maxSize,
		splitSize:       splitSize,
		batchSplitLimit: batchSplitLimit,
	}
}

func (checker *sizeSplitChecker) onKv(key []byte, item *engine_util.CFItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	overLimit := uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	if checker.currentSize > checker.splitSize && !overLimit {
		checker.splitKeys = append(checker.splitKeys, safeCopy(key))
		// If for previous onKv(), checker.current_size == checker.split_size,
		// the split key would be pushed this time, but the entry size for this time should not be ignored.
		if checker.currentSize-size == checker.splitSize {
			checker.currentSize = size
		} else {
			checker.currentSize = 0
		}
		overLimit = uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	}
	// For a large region, scan over the range maybe cost too much time,
	// so limit the number of produced splitKeys for one batch.
	// Also need to scan over checker.maxSize for last part.
	return overLimit && checker.currentSize+checker.splitSize >= checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKeys() [][]byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize+checker.splitSize < checker.maxSize {
		splitKeyLen := len(checker.splitKeys)
		if splitKeyLen != 0 {
			checker.splitKeys = checker.splitKeys[:splitKeyLen-1]
		}
	}
	keys := checker.splitKeys
	checker.splitKeys = nil
	return keys
}

type snapContext struct {
	engines   *engine_util.Engines
	batchSize uint64
	mgr       *SnapManager
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
func (snapCtx *snapContext) cleanUpOriginData(regionState *rspb.RegionLocalState, status *JobStatus) error {
	startKey, endKey := regionState.GetRegion().StartKey, regionState.GetRegion().EndKey
	if err := checkAbort(status); err != nil {
		return err
	}
	if err := engine_util.DeleteRange(snapCtx.engines.Kv, startKey, endKey); err != nil {
		return err
	}
	if err := checkAbort(status); err != nil {
		return err
	}
	return nil
}

// applySnap applies snapshot data of the Region.
func (snapCtx *snapContext) applySnap(regionId uint64, status *JobStatus) error {
	log.Infof("begin apply snap data. [regionId: %d]", regionId)
	if err := checkAbort(status); err != nil {
		return err
	}

	regionKey := RegionStateKey(regionId)
	regionState, err := getRegionLocalState(snapCtx.engines.Kv, regionId)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get regionState from %v", regionKey))
	}

	// Clean up origin data
	if err := snapCtx.cleanUpOriginData(regionState, status); err != nil {
		return err
	}

	applyState, err := getApplyState(snapCtx.engines.Kv, regionId)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get raftState from %v", ApplyStateKey(regionId)))
	}
	snapKey := SnapKey{RegionID: regionId, Index: applyState.truncatedIndex, Term: applyState.truncatedTerm}
	snapCtx.mgr.Register(snapKey, SnapEntryApplying)
	defer snapCtx.mgr.Deregister(snapKey, SnapEntryApplying)

	snap, err := snapCtx.mgr.GetSnapshotForApplying(snapKey)
	if err != nil {
		return errors.New(fmt.Sprintf("missing snapshot file %s", snap.Path()))
	}

	t := time.Now()
	applyOptions := newApplyOptions(snapCtx.engines.Kv, regionState.GetRegion(), status)
	if err := snap.Apply(*applyOptions); err != nil {
		return err
	}

	regionState.State = rspb.PeerState_Normal
	wb := new(engine_util.WriteBatch)
	wb.SetMsg(RegionStateKey(regionId), regionState)
	wb.Delete(SnapshotRaftStateKey(regionId))
	if err := wb.WriteToKV(snapCtx.engines.Kv); err != nil {
		log.Errorf("update region status failed: %s", err)
	}

	log.Infof("applying new data. [regionId: %d, timeTakes: %v]", regionId, time.Now().Sub(t))
	return nil
}

// handleApply tries to apply the snapshot of the specified Region. It calls `applySnap` to do the actual work.
func (snapCtx *snapContext) handleApply(regionId uint64, status *JobStatus) {
	atomic.CompareAndSwapUint32(status, JobStatus_Pending, JobStatus_Running)
	err := snapCtx.applySnap(regionId, status)
	switch err.(type) {
	case nil:
		atomic.SwapUint32(status, JobStatus_Finished)
	case applySnapAbortError:
		log.Warnf("applying snapshot is aborted. [regionId: %d]", regionId)
		y.Assert(atomic.SwapUint32(status, JobStatus_Cancelled) == JobStatus_Cancelling)
	default:
		log.Errorf("failed to apply snap!!!. err: %v", err)
		atomic.SwapUint32(status, JobStatus_Failed)
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

type regionApplyState struct {
	localState *rspb.RegionLocalState
	tableCount int
}

type regionTaskHandler struct {
	ctx         *snapContext
	applyStates []regionApplyState
}

func newRegionTaskHandler(engines *engine_util.Engines, mgr *SnapManager) *regionTaskHandler {
	return &regionTaskHandler{
		ctx: &snapContext{
			engines: engines,
			mgr:     mgr,
		},
	}
}

func (r *regionTaskHandler) Handle(t worker.Task) {
	task := t.Data.(*regionTask)
	switch t.Tp {
	case worker.TaskTypeRegionGen:
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		r.ctx.handleGen(task.regionId, task.notifier)
	case worker.TaskTypeRegionApply:
		r.ctx.handleApply(task.regionId, task.status)
	case worker.TaskTypeRegionDestroy:
		r.ctx.cleanUpRange(task.regionId, task.startKey, task.endKey)
	}
}

func (r *regionTaskHandler) shutdown() {
	// todo, currently it is a a place holder.
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCTaskHandler) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {

	// Find the raft log idx range needed to be gc.
	firstIdx := startIdx
	if firstIdx == 0 {
		firstIdx = endIdx
		err := raftDb.View(func(txn *badger.Txn) error {
			startKey := RaftLogKey(regionId, 0)
			ite := txn.NewIterator(badger.DefaultIteratorOptions)
			defer ite.Close()
			if ite.Seek(startKey); ite.Valid() {
				var err error
				if firstIdx, err = RaftLogIndex(ite.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	if firstIdx >= endIdx {
		log.Infof("no need to gc, [regionId: %d]", regionId)
		return 0, nil
	}

	raftWb := engine_util.WriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		key := RaftLogKey(regionId, idx)
		raftWb.Delete(key)
	}
	// todo, disable WAL here.
	if raftWb.Len() != 0 {
		if err := raftWb.WriteToRaft(raftDb); err != nil {
			return 0, err
		}
	}
	return endIdx - firstIdx, nil
}

func (r *raftLogGCTaskHandler) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCTaskHandler) Handle(t worker.Task) {
	logGcTask := t.Data.(*raftLogGCTask)
	log.Debugf("execute gc log. [regionId: %d, endIndex: %d]", logGcTask.regionID, logGcTask.endIdx)
	collected, err := r.gcRaftLog(logGcTask.raftEngine, logGcTask.regionID, logGcTask.startIdx, logGcTask.endIdx)
	if err != nil {
		log.Errorf("failed to gc. [regionId: %d, collected: %d, err: %v]", logGcTask.regionID, collected, err)
	} else {
		log.Debugf("collected log entries. [regionId: %d, entryCount: %d]", logGcTask.regionID, collected)
	}
	r.reportCollected(collected)
}
