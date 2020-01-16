package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/lockstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
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
	callback  *Callback
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

type pdValidatePeerTask struct {
	region *metapb.Region
	peer   *metapb.Peer
}

type readStats map[uint64]flowStats

type pdDestroyPeerTask struct {
	regionID uint64
}

type flowStats struct {
	readBytes uint64
	readKeys  uint64
}

type compactTask struct {
	keyRange keyRange
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
		msg := Msg{
			Type:     MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &MsgSplitRegion{
				RegionEpoch: regionEpoch,
				SplitKeys:   keys,
				Callback:    NewCallback(),
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

type pendingDeleteRanges struct {
	ranges *lockstore.MemStore
}

func (pendDelRanges *pendingDeleteRanges) insert(regionId uint64, startKey, endKey []byte, timeout time.Time) {
	if len(pendDelRanges.findOverlapRanges(startKey, endKey)) != 0 {
		panic(fmt.Sprintf("[region %d] register deleting data in [%v, %v) failed due to overlap", regionId, startKey, endKey))
	}
	peerInfo := newStalePeerInfo(regionId, endKey, timeout)
	pendDelRanges.ranges.Insert(startKey, peerInfo.data)
}

// remove removes and returns the peer info with the `start_key`.
func (pendDelRanges *pendingDeleteRanges) remove(startKey []byte) *stalePeerInfo {
	value := pendDelRanges.ranges.Get(startKey, nil)
	if value != nil {
		pendDelRanges.ranges.Delete(startKey)
		return &stalePeerInfo{data: safeCopy(value)}
	}
	return nil
}

// timeoutRanges returns all timeout ranges info.
func (pendDelRanges *pendingDeleteRanges) timeoutRanges(now time.Time) (ranges []delRangeHolder) {
	ite := pendDelRanges.ranges.NewIterator()
	for ite.Next(); ite.Valid(); ite.Next() {
		startKey := safeCopy(ite.Key())
		peerInfo := stalePeerInfo{data: safeCopy(ite.Value())}
		if peerInfo.timeout().Before(now) {
			ranges = append(ranges, delRangeHolder{
				startKey: startKey,
				endKey:   peerInfo.endKey(),
				regionId: peerInfo.regionId(),
			})
		}
	}
	return
}

type stalePeerInfo struct {
	data []byte
}

func newStalePeerInfo(regionId uint64, endKey []byte, timeout time.Time) stalePeerInfo {
	s := stalePeerInfo{data: make([]byte, 16+len(endKey))}
	s.setRegionId(regionId)
	s.setTimeout(timeout)
	s.setEndKey(endKey)
	return s
}

func (s stalePeerInfo) regionId() uint64 {
	return binary.LittleEndian.Uint64(s.data[:8])
}

func (s stalePeerInfo) timeout() time.Time {
	return time.Unix(0, int64(binary.LittleEndian.Uint64(s.data[8:16])))
}

func (s stalePeerInfo) endKey() []byte {
	return s.data[16:]
}

func (s stalePeerInfo) setRegionId(regionId uint64) {
	binary.LittleEndian.PutUint64(s.data[:8], regionId)
}

func (s stalePeerInfo) setTimeout(timeout time.Time) {
	binary.LittleEndian.PutUint64(s.data[8:16], uint64(timeout.UnixNano()))
}

func (s stalePeerInfo) setEndKey(endKey []byte) {
	copy(s.data[16:], endKey)
}

type delRangeHolder struct {
	startKey []byte
	endKey   []byte
	regionId uint64
}

// findOverlapRanges finds ranges that overlap with [start_key, end_key).
func (pendDelRanges *pendingDeleteRanges) findOverlapRanges(startKey, endKey []byte) (ranges []delRangeHolder) {
	if exceedEndKey(startKey, endKey) {
		return nil
	}
	ite := pendDelRanges.ranges.NewIterator()
	// find the first range that may overlap with [start_key, end_key)
	if ite.SeekForExclusivePrev(startKey); ite.Valid() {
		peerInfo := stalePeerInfo{data: safeCopy(ite.Value())}
		if bytes.Compare(peerInfo.endKey(), startKey) > 0 {
			ranges = append(ranges, delRangeHolder{startKey: safeCopy(ite.Key()), endKey: peerInfo.endKey(), regionId: peerInfo.regionId()})
		}
	}
	// Find the rest ranges that overlap with [start_key, end_key)
	for ite.Next(); ite.Valid(); ite.Next() {
		peerInfo := stalePeerInfo{data: safeCopy(ite.Value())}
		startKey := safeCopy(ite.Key())
		if exceedEndKey(startKey, endKey) {
			break
		}
		ranges = append(ranges, delRangeHolder{startKey: startKey, endKey: peerInfo.endKey(), regionId: peerInfo.regionId()})
	}
	return
}

// drainOverlapRanges gets ranges that overlap with [start_key, end_key).
func (pendDelRanges *pendingDeleteRanges) drainOverlapRanges(startKey, endKey []byte) []delRangeHolder {
	ranges := pendDelRanges.findOverlapRanges(startKey, endKey)
	for _, r := range ranges {
		y.Assert(pendDelRanges.ranges.Delete(r.startKey))
	}
	return ranges
}

type snapContext struct {
	engines             *engine_util.Engines
	batchSize           uint64
	mgr                 *SnapManager
	cleanStalePeerDelay time.Duration
	pendingDeleteRanges *pendingDeleteRanges
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
	startKey := EncStartKey(regionState.GetRegion())
	endKey := EncEndKey(regionState.GetRegion())
	if err := checkAbort(status); err != nil {
		return err
	}
	snapCtx.cleanUpOverlapRanges(startKey, endKey)
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
func (snapCtx *snapContext) handleApply(regionId uint64, status *JobStatus) error {
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
	return err
}

// cleanupOverlapRanges gets the overlapping ranges and cleans them up.
func (snapCtx *snapContext) cleanUpOverlapRanges(startKey, endKey []byte) {
	overlapRanges := snapCtx.pendingDeleteRanges.drainOverlapRanges(startKey, endKey)
	for _, r := range overlapRanges {
		snapCtx.cleanUpRange(r.regionId, r.startKey, r.endKey)
	}
}

// insertPendingDeleteRange inserts a new pending range, and it will be cleaned up with some delay.
func (snapCtx *snapContext) insertPendingDeleteRange(regionId uint64, startKey, endKey []byte) bool {
	if int64(snapCtx.cleanStalePeerDelay.Seconds()) == 0 {
		return false
	}
	snapCtx.cleanUpOverlapRanges(startKey, endKey)
	log.Infof("register deleting data in range. [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	timeout := time.Now().Add(snapCtx.cleanStalePeerDelay)
	snapCtx.pendingDeleteRanges.insert(regionId, startKey, endKey, timeout)
	return true
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
	ctx *snapContext
	// we may delay some apply tasks if level 0 files to write stall threshold,
	// pending_applies records all delayed apply worker.Task, and will check again later
	pendingApplies []worker.Task

	applyStates []regionApplyState
}

func newRegionTaskHandler(engines *engine_util.Engines, mgr *SnapManager, batchSize uint64, cleanStalePeerDelay time.Duration) *regionTaskHandler {
	return &regionTaskHandler{
		ctx: &snapContext{
			engines:             engines,
			mgr:                 mgr,
			batchSize:           batchSize,
			cleanStalePeerDelay: cleanStalePeerDelay,
			pendingDeleteRanges: &pendingDeleteRanges{
				ranges: lockstore.NewMemStore(4096),
			},
		},
	}
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionTaskHandler) handlePendingApplies() {
	for len(r.pendingApplies) > 0 {
		// Try to apply worker.Task, if apply failed, throw away this worker.Task and let sender retry
		apply := r.pendingApplies[0]
		r.pendingApplies = r.pendingApplies[1:]

		task := apply.Data.(*regionTask)
		err := r.ctx.handleApply(task.regionId, task.status)
		if err != nil {
			log.Error(err)
			continue
		}
	}
}

func (r *regionTaskHandler) Handle(t worker.Task) {
	switch t.Tp {
	case worker.TaskTypeRegionGen:
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		regionTask := t.Data.(*regionTask)
		r.ctx.handleGen(regionTask.regionId, regionTask.notifier)
	case worker.TaskTypeRegionApply:
		// To make sure applying snapshots in order.
		r.pendingApplies = append(r.pendingApplies, t)
		r.handlePendingApplies()
	case worker.TaskTypeRegionDestroy:
		// Try to delay the range deletion because
		// there might be a coprocessor request related to this range
		regionTask := t.Data.(regionTask)
		if !r.ctx.insertPendingDeleteRange(regionTask.regionId, regionTask.startKey, regionTask.endKey) {
			// Use delete files
			r.ctx.cleanUpRange(regionTask.regionId, regionTask.startKey, regionTask.endKey)
		}
	}
}

func (r *regionTaskHandler) shutdown() {
	// todo, currently it is a a place holder.
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

// In our tests, we found that if the batch size is too large, running deleteAllInRange will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
const MaxDeleteBatchSize int = 32 * 1024

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
