package raftstore

import (
	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"sync"
	"time"
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
	tasktypePDDestroyPeer      taskType = 108

	taskTypeCompact         taskType = 201
	taskTypeCheckAndCompact taskType = 202

	taskTypeRegionGen   taskType = 401
	taskTypeRegionApply taskType = 402
	/// Destroy data between [start_key, end_key).
	///
	/// The deletion may and may not succeed.
	taskTypeRegionDestroy taskType = 403
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
	callback    Callback
}

type pdAskBatchSplitTask struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    Callback
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

func (w *worker) start(runner taskRunner) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			task := <-w.receiver
			if task.tp == taskTypeStop {
				return
			}
			runner.run(task)
		}
	}()
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

type splitCheckRunner struct {
	engine          *badger.DB
	router          *router
	coprocessorHost *CoprocessorHost
}

func (r *splitCheckRunner) run(t task) {
	// TODO: stub
}

type pendingDeleteRanges struct {
	ranges *lockstore.MemStore
}

type snapContext struct {
	engiens             *Engines
	batchSize           int
	mgr                 *SnapManager
	cleanStalePeerDelay time.Duration
	pendingDeleteRanges *pendingDeleteRanges
}

type regionRunner struct {
	ctx *snapContext
	// we may delay some apply tasks if level 0 files to write stall threshold,
	// pending_applies records all delayed apply task, and will check again later
	pendingApplies []task
}

func newRegionRunner(engines *Engines, mgr *SnapManager, batchSize uint64, cleanStalePeerDelay time.Duration) *regionRunner {
	return nil // TODO: stub
}

func (r *regionRunner) run(t task) {
	// TODO: stub
}

type raftLogGCRunner struct {
}

func (r *raftLogGCRunner) run(t task) {
	// TODO: stub
}

type compactRunner struct {
	engine *badger.DB
}

func (r *compactRunner) run(t task) {
	// TODO: stub
}

type pdRunner struct {
	storeID   uint64
	pdClient  pd.Client
	router    *router
	db        *badger.DB
	scheduler chan<- task
}

func newPDRunner(storeID uint64, pdClient pd.Client, router *router, db *badger.DB, scheduler chan<- task) *pdRunner {
	return nil // TODO: stub
}

func (r *pdRunner) run(t task) {
	// TODO: stub
}

type computeHashRunner struct {
	router *router
}

func (r *computeHashRunner) run(t task) {
	// TODO: stub
}
