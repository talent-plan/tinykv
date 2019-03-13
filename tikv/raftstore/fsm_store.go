package raftstore

import (
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"sync"
)

type storeMeta struct {
	// region end key -> region ID
	regionRanges           *lockstore.MemStore
	regions                map[uint64]*metapb.Region
	pendingCrossSnap       map[uint64]*metapb.RegionEpoch
	pendingSnapshotRegions []*metapb.Region
}

type PollContext struct {
	Cfg             *Config
	CoprocessorHost *CoprocessorHost
	engine          *Engines
	dbBundle        *DBBundle
	applyRouter     *ApplyRouter
	needFlushTrans  bool
	ReadyRes        []ReadyICPair
	kvWB            *WriteBatch
	raftWB          *WriteBatch
	syncLog         bool
	storeMeta       *storeMeta
	storeMetaLock   sync.Mutex
	snapMgr         *SnapManager
	pendingCount    int
	hasReady        bool
	router          *router
	tickDriverCh    chan<- uint64
	trans           Transport
	queuedSnaps     map[uint64]struct{}
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
