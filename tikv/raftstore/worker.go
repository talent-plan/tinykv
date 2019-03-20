package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type taskType int64

const (
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

	taskTypeCleanUpSSTDelete   taskType = 301
	taskTypeCleanUpSSTValidate taskType = 302

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

type cleanUpSSTTask struct {
	ssts []*import_sstpb.SSTMeta
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
	scheduler chan<- task
}
