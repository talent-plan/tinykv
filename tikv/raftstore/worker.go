package raftstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/codec"
)

type taskType int64

const (
	taskTypeStop        taskType = 0
	taskTypeRaftLogGC   taskType = 1
	taskTypeSplitCheck  taskType = 2
	taskTypeComputeHash taskType = 3

	taskTypePDAskSplit         taskType = 101
	taskTypePDAskBatchSplit    taskType = 102
	taskTypePDHeartbeat        taskType = 103
	taskTypePDStoreHeartbeat   taskType = 104
	taskTypePDReportBatchSplit taskType = 105
	taskTypePDValidatePeer     taskType = 106
	taskTypePDReadStats        taskType = 107
	taskTypePDDestroyPeer      taskType = 108

	taskTypeCompact         taskType = 201
	taskTypeCheckAndCompact taskType = 202

	taskTypeRegionGen   taskType = 401
	taskTypeRegionApply taskType = 402
	/// Destroy data between [start_key, end_key).
	///
	/// The deletion may and may not succeed.
	taskTypeRegionDestroy taskType = 403

	taskTypeResolveAddr taskType = 501

	taskTypeSnapSend taskType = 601
	taskTypeSnapRecv taskType = 602
)

type task struct {
	tp   taskType
	data interface{}
}

type regionTask struct {
	regionId uint64
	notifier chan<- *eraftpb.Snapshot
	status   *JobStatus
	startKey []byte
	endKey   []byte
	redoIdx  uint64
}

type raftLogGCTask struct {
	raftEngine *badger.DB
	regionID   uint64
	startIdx   uint64
	endIdx     uint64
}

type splitCheckTask struct {
	region    *metapb.Region
	autoSplit bool
	policy    pdpb.CheckPolicy
}

type computeHashTask struct {
	index  uint64
	region *metapb.Region
	snap   *DBSnapshot
}

type pdAskSplitTask struct {
	region   *metapb.Region
	splitKey []byte
	peer     *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    *Callback
}

type pdAskBatchSplitTask struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    *Callback
}

type pdRegionHeartbeatTask struct {
	region          *metapb.Region
	peer            *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	approximateSize *uint64
	approximateKeys *uint64
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
	region      *metapb.Region
	peer        *metapb.Peer
	mergeSource *uint64
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

type checkAndCompactTask struct {
	ranges                    []keyRange
	tombStoneNumThreshold     uint64 // The minimum RocksDB tombstones a range that need compacting has
	tombStonePercentThreshold uint64
}

type resolveAddrTask struct {
	storeID  uint64
	callback func(addr string, err error)
}

type sendSnapTask struct {
	addr     string
	msg      *raft_serverpb.RaftMessage
	callback func(error)
}

type recvSnapTask struct {
	stream   tikvpb.Tikv_SnapshotServer
	callback func(error)
}

type worker struct {
	name      string
	scheduler chan<- task
	receiver  <-chan task
	closeCh   chan struct{}
	wg        *sync.WaitGroup
}

type taskRunner interface {
	run(t task)
}

type starter interface {
	start()
}

func (w *worker) start(runner taskRunner) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if s, ok := runner.(starter); ok {
			s.start()
		}
		for {
			task := <-w.receiver
			if task.tp == taskTypeStop {
				return
			}
			runner.run(task)
		}
	}()
}

func (w *worker) stop() {
	w.scheduler <- task{tp: taskTypeStop}
}

const defaultWorkerCapacity = 128

func newWorker(name string, wg *sync.WaitGroup) *worker {
	ch := make(chan task, defaultWorkerCapacity)
	return &worker{
		scheduler: (chan<- task)(ch),
		receiver:  (<-chan task)(ch),
		name:      name,
		wg:        wg,
	}
}

type splitCheckKeyEntry struct {
	key       []byte
	valueSize uint64
}

func (keyEntry *splitCheckKeyEntry) entrySize() uint64 {
	return uint64(len(keyEntry.key)) + keyEntry.valueSize
}

type splitCheckRunner struct {
	engine          *badger.DB
	router          *router
	coprocessorHost *CoprocessorHost
}

func newSplitCheckRunner(engine *badger.DB, router *router, host *CoprocessorHost) *splitCheckRunner {
	return &splitCheckRunner{
		engine:          engine,
		router:          router,
		coprocessorHost: host,
	}
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckRunner) run(t task) {
	spCheckTask := t.data.(*splitCheckTask)
	region := spCheckTask.region
	regionId := region.Id
	startKey := region.StartKey
	endKey := region.EndKey
	log.Debugf("executing split check task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	host := r.coprocessorHost.newSplitCheckerHost(region, r.engine, spCheckTask.autoSplit,
		spCheckTask.policy)
	if host.skip() {
		log.Debugf("skip split check, [regionId : %d]", regionId)
		return
	}
	var keys [][]byte
	var err error
	switch host.policy() {
	case pdpb.CheckPolicy_SCAN:
		if keys, err = r.scanSplitKeys(host, region, startKey, endKey); err != nil {
			log.Errorf("failed to scan split key: [regionId: %d, err: %v]", regionId, err)
			return
		}
	case pdpb.CheckPolicy_APPROXIMATE:
		// todo, currently, use scan split keys as place holder.
		if keys, err = r.scanSplitKeys(host, region, startKey, endKey); err != nil {
			log.Errorf("failed to scan split key: [regionId: %d, err: %v]", regionId, err)
			return
		}
	}
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

func exceedEndKey(current, endKey []byte) bool {
	return bytes.Compare(current, endKey) >= 0
}

/// scanSplitKeys gets the split keys by scanning the range.
func (r *splitCheckRunner) scanSplitKeys(spCheckerHost *splitCheckerHost, region *metapb.Region,
	startKey []byte, endKey []byte) ([][]byte, error) {
	txn := r.engine.NewTransaction(false)
	reader := dbreader.NewDBReader(startKey, endKey, txn, 0)
	ite := reader.GetIter()
	defer reader.Close()
	for ite.Seek(startKey); ite.Valid(); ite.Next() {
		item := ite.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		if value, err := item.Value(); err == nil {
			if (spCheckerHost.onKv(region, splitCheckKeyEntry{key: key, valueSize: uint64(len(value))})) {
				break
			}
		} else {
			return nil, errors.Trace(err)
		}
	}
	return spCheckerHost.splitKeys(), nil
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
	engiens             *Engines
	batchSize           uint64
	mgr                 *SnapManager
	cleanStalePeerDelay time.Duration
	pendingDeleteRanges *pendingDeleteRanges
}

// handleGen handles the task of generating snapshot of the Region. It calls `generateSnap` to do the actual work.
func (snapCtx *snapContext) handleGen(regionId, redoIdx uint64, notifier chan<- *eraftpb.Snapshot) {
	if err := snapCtx.generateSnap(regionId, redoIdx, notifier); err != nil {
		log.Errorf("failed to generate snapshot!!!, [regionId: %d, err : %v]", regionId, err)
	}
}

// generateSnap generates the snapshots of the Region
func (snapCtx *snapContext) generateSnap(regionId, redoIdx uint64, notifier chan<- *eraftpb.Snapshot) error {
	// do we need to check leader here?
	snap, err := doSnapshot(snapCtx.engiens, snapCtx.mgr, regionId, redoIdx)
	if err != nil {
		return err
	}
	notifier <- snap
	return nil
}

// cleanUpOriginData clear up the region data before applying snapshot
func (snapCtx *snapContext) cleanUpOriginData(regionState *rspb.RegionLocalState, status *JobStatus) error {
	startKey := EncStartKey(regionState.GetRegion())
	endKey := EncEndKey(regionState.GetRegion())
	if err := checkAbort(status); err != nil {
		return err
	}
	snapCtx.cleanUpOverlapRanges(startKey, endKey)
	if err := deleteRange(snapCtx.engiens.kv, startKey, endKey); err != nil {
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
	regionState, err := getRegionLocalState(snapCtx.engiens.kv.db, regionId)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get regionState from %v", regionKey))
	}

	// Clean up origin data
	if err := snapCtx.cleanUpOriginData(regionState, status); err != nil {
		return err
	}

	applyState, err := getApplyState(snapCtx.engiens.kv.db, regionId)
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
	applyOptions := newApplyOptions(snapCtx.engiens.kv, regionState.GetRegion(), status, int(snapCtx.batchSize))
	if err := snap.Apply(*applyOptions); err != nil {
		return err
	}

	wb := new(WriteBatch)
	regionState.State = rspb.PeerState_Normal
	if err := wb.SetMsg(regionKey, regionState); err != nil {
		return err
	}
	wb.Delete(SnapshotRaftStateKey(regionId))
	if err := wb.WriteToKV(snapCtx.engiens.kv); err != nil {
		panic(fmt.Sprintf("Region %d faile to save applySnap result: %v", regionId, err))
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

/// ingestMaybeStall checks the number of files at level 0 to avoid write stall after ingesting sst.
/// Returns true if the ingestion causes write stall.
func (snapCtx *snapContext) ingestMaybeStall() bool {
	for _, cf := range snapshotCFs {
		if plainFileUsed(cf) {
			continue
		}
		// todo, related to cf.
	}
	return false
}

// cleanupOverlapRanges gets the overlapping ranges and cleans them up.
func (snapCtx *snapContext) cleanUpOverlapRanges(startKey, endKey []byte) {
	overlapRanges := snapCtx.pendingDeleteRanges.drainOverlapRanges(startKey, endKey)
	useDeleteFiles := false
	for _, r := range overlapRanges {
		snapCtx.cleanUpRange(r.regionId, r.startKey, r.endKey, useDeleteFiles)
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
func (snapCtx *snapContext) cleanUpRange(regionId uint64, startKey, endKey []byte, useDeleteFiles bool) {
	if useDeleteFiles {
		if err := deleteAllFilesInRange(snapCtx.engiens.kv, startKey, endKey); err != nil {
			log.Errorf("failed to delete files in range, [regionId: %d, startKey: %s, endKey: %s, err: %v]", regionId,
				hex.EncodeToString(startKey), hex.EncodeToString(endKey), err)
			return
		}
	}
	if err := deleteRange(snapCtx.engiens.kv, startKey, endKey); err != nil {
		log.Errorf("failed to delete data in range, [regionId: %d, startKey: %s, endKey: %s, err: %v]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey), err)
	} else {
		log.Infof("succeed in deleting data in range. [regionId: %d, startKey: %s, endKey: %s]", regionId,
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}
}

type regionRunner struct {
	ctx *snapContext
	// we may delay some apply tasks if level 0 files to write stall threshold,
	// pending_applies records all delayed apply task, and will check again later
	pendingApplies []task
}

func newRegionRunner(engines *Engines, mgr *SnapManager, batchSize uint64, cleanStalePeerDelay time.Duration) *regionRunner {
	return &regionRunner{
		ctx: &snapContext{
			engiens:             engines,
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
func (r *regionRunner) handlePendingApplies() {
	for len(r.pendingApplies) != 0 {
		// Should not handle too many applies than the number of files that can be ingested.
		// Check level 0 every time because we can not make sure how does the number of level 0 files change.
		if r.ctx.ingestMaybeStall() {
			break
		}
		task := r.pendingApplies[0].data.(*regionTask)
		r.pendingApplies = r.pendingApplies[1:]
		r.ctx.handleApply(task.regionId, task.status)
	}
}

func (r *regionRunner) run(t task) {
	switch t.tp {
	case taskTypeRegionGen:
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		regionTask := t.data.(*regionTask)
		r.ctx.handleGen(regionTask.regionId, regionTask.redoIdx, regionTask.notifier)
	case taskTypeRegionApply:
		// To make sure applying snapshots in order.
		r.pendingApplies = append(r.pendingApplies, t)
		r.handlePendingApplies()
	case taskTypeRegionDestroy:
		// Try to delay the range deletion because
		// there might be a coprocessor request related to this range
		regionTask := t.data.(regionTask)
		if !r.ctx.insertPendingDeleteRange(regionTask.regionId, regionTask.startKey, regionTask.endKey) {
			// Use delete files
			r.ctx.cleanUpRange(regionTask.regionId, regionTask.startKey, regionTask.endKey, false)
		}
	}
}

func (r *regionRunner) shutdown() {
	// todo, currently it is a a place holder.
}

type raftLogGcTaskRes uint64

type raftLogGCRunner struct {
	taskResCh chan<- raftLogGcTaskRes
}

// In our tests, we found that if the batch size is too large, running deleteAllInRange will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
const MaxDeleteBatchSize int = 32 * 1024

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCRunner) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {

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

	raftWb := WriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		key := RaftLogKey(regionId, idx)
		raftWb.Delete(key)
		if raftWb.size >= MaxDeleteBatchSize {
			// Avoid large write batch to reduce latency.
			if err := raftWb.WriteToRaft(raftDb); err != nil {
				return 0, err
			}
			raftWb.Reset()
		}
	}
	// todo, disable WAL here.
	if raftWb.Len() != 0 {
		if err := raftWb.WriteToRaft(raftDb); err != nil {
			return 0, err
		}
	}
	return endIdx - firstIdx, nil
}

func (r *raftLogGCRunner) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCRunner) run(t task) {
	logGcTask := t.data.(*raftLogGCTask)
	log.Debugf("execute gc log. [regionId: %d, endIndex: %d]", logGcTask.regionID, logGcTask.endIdx)
	collected, err := r.gcRaftLog(logGcTask.raftEngine, logGcTask.regionID, logGcTask.startIdx, logGcTask.endIdx)
	if err != nil {
		log.Errorf("failed to gc. [regionId: %d, collected: %d, err: %v]", logGcTask.regionID, collected, err)
	} else {
		log.Debugf("collected log entries. [regionId: %d, entryCount: %d]", logGcTask.regionID, collected)
	}
	r.reportCollected(collected)
}

type compactRunner struct {
	engine *badger.DB
}

func (r *compactRunner) run(t task) {
	// TODO: stub
}

type computeHashRunner struct {
	router *router
}

func (r *computeHashRunner) run(t task) {
	// TODO: stub
}
