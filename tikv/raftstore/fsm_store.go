package raftstore

import (
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"sync"
)

type storeMeta struct {
	/// region end key -> region ID
	regionRanges *lockstore.MemStore
	/// region_id -> region
	regions map[uint64]*metapb.Region
	/// `MsgRequestPreVote` or `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
	/// such Region in this store now. So the messages are recorded temporarily and will be handled later.
	pendingVotes []*rspb.RaftMessage
	/// The regions with pending snapshots.
	pendingSnapshotRegions []*metapb.Region
	/// A marker used to indicate the peer of a Region has received a merge target message and waits to be destroyed.
	/// target_region_id -> (source_region_id -> merge_target_epoch)
	pendingMergeTargets map[uint64]map[uint64]*metapb.RegionEpoch
	/// An inverse mapping of `pending_merge_targets` used to let source peer help target peer to clean up related entry.
	/// source_region_id -> target_region_id
	targetsMap map[uint64]uint64
	/// In raftstore, the execute order of `PrepareMerge` and `CommitMerge` is not certain because of the messages
	/// belongs two regions. To make them in order, `PrepareMerge` will set this structure and `CommitMerge` will retry
	/// later if there is no related lock.
	/// source_region_id -> (version, BiLock).
	mergeLocks map[uint64]*mergeLock
}

func (m *storeMeta) setRegion(host *CoprocessorHost, region *metapb.Region, peer *Peer) {
	m.regions[region.Id] = CloneRegion(region)
	peer.SetRegion(host, region)
}

type mergeLock struct {
}

type PollContext struct {
	Cfg                  *Config
	CoprocessorHost      *CoprocessorHost
	engine               *Engines
	dbBundle             *DBBundle
	applyRouter          *ApplyRouter
	needFlushTrans       bool
	ReadyRes             []ReadyICPair
	kvWB                 *WriteBatch
	raftWB               *WriteBatch
	syncLog              bool
	storeMeta            *storeMeta
	storeMetaLock        sync.Mutex
	snapMgr              *SnapManager
	pendingCount         int
	hasReady             bool
	router               *router
	tickDriverCh         chan<- uint64
	trans                Transport
	queuedSnaps          map[uint64]struct{}
	pdScheduler          chan<- pdTask
	raftLogGCScheduler   chan<- raftLogGCTask
	store                *metapb.Store
	regionScheduler      chan<- *RegionTask
	splitCheckScheduler  chan<- splitCheckTask
	computeHashScheduler chan<- computeHashTask
	cleanUpSSTScheculer  chan<- cleanUpSSTTask
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

func (pc *PollContext) KVWB() *WriteBatch {
	return pc.kvWB
}

func (pc *PollContext) RaftWB() *WriteBatch {
	return pc.raftWB
}

func (pc *PollContext) SyncLog() bool {
	return pc.syncLog
}

func (pc *PollContext) SetSyncLog(sync bool) {
	pc.syncLog = sync
}

func (pc *PollContext) handleStaleMsg(msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool, targetRegion *metapb.Region) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    ClonePeer(fromPeer),
		ToPeer:      ClonePeer(toPeer),
		RegionEpoch: CloneRegionEpoch(curEpoch),
	}
	if targetRegion != nil {
		gcMsg.MergeTarget = targetRegion
	} else {
		gcMsg.IsTombstone = true
	}
	if err := pc.trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}
