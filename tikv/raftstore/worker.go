package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

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

const (
	cleanUpSSTTaskDelete   = 1
	cleanUpSSTTaskValidate = 2
)

type cleanUpSSTTask struct {
	tp   int
	ssts []*import_sstpb.SSTMeta
}

type pdTaskType int64

const (
	pdTaskAskSplit         pdTaskType = 1
	pdTaskAskBatchSplit    pdTaskType = 2
	pdTaskHeartbeat        pdTaskType = 3
	pdTaskStoreHeartbeat   pdTaskType = 4
	pdTaskReportBatchSplit pdTaskType = 5
	pdTaskValidatePeer     pdTaskType = 6
	pdTaskReadStats        pdTaskType = 7
	pdTaskDestroyPeer      pdTaskType = 8
)

type pdAskSplit struct {
	region   *metapb.Region
	splitKey []byte
	peer     *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    Callback
}

type pdAskBatchSplit struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    Callback
}

type pdRegionHeartbeat struct {
	region          *metapb.Region
	peer            *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	approximateSize *uint64
	approximateKeys *uint64
}

type pdStoreHeartbeat struct {
	stats    *pdpb.StoreStats
	engine   *badger.DB
	path     string
	capacity uint64
}

type pdReportBatchSplit struct {
	regions []*metapb.Region
}

type pdValidatePeer struct {
	region      *metapb.Region
	peer        *metapb.Peer
	mergeSource *uint64
}

type readStats map[uint64]flowStats

type flowStats struct {
	readBytes uint64
	readKeys  uint64
}

type pdTask struct {
	tp   pdTaskType
	data interface{}
}
