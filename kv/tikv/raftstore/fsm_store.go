package raftstore

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/lockstore"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

type StoreTick int

const (
	StoreTickPdStoreHeartbeat StoreTick = 1
	StoreTickSnapGC           StoreTick = 2
)

type storeMeta struct {
	/// region end key -> region ID
	regionRanges *lockstore.MemStore
	/// region_id -> region
	regions map[uint64]*metapb.Region
	/// `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
	/// such Region in this store now. So the messages are recorded temporarily and will be handled later.
	pendingVotes []*rspb.RaftMessage
	/// The regions with pending snapshots.
	pendingSnapshotRegions []*metapb.Region
}

func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionRanges: lockstore.NewMemStore(32 * 1024),
		regions:      map[uint64]*metapb.Region{},
	}
}

func (m *storeMeta) setRegion(region *metapb.Region, peer *Peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

type GlobalContext struct {
	cfg                  *config.Config
	engine               *engine_util.Engines
	store                *metapb.Store
	storeMeta            *storeMeta
	storeMetaLock        *sync.RWMutex
	snapMgr              *snap.SnapManager
	router               *router
	trans                Transport
	pdTaskSender         chan<- worker.Task
	regionTaskSender     chan<- worker.Task
	raftLogGCTaskSender  chan<- worker.Task
	splitCheckTaskSender chan<- worker.Task
	pdClient             pd.Client
	tickDriverSender     chan uint64
}

type StoreContext struct {
	*GlobalContext
	applyingSnapCount *uint64
}

type RaftContext struct {
	*GlobalContext
	applyMsgs    *applyMsgs
	ReadyRes     []*ReadyICPair
	kvWB         *engine_util.WriteBatch
	raftWB       *engine_util.WriteBatch
	pendingCount int
	hasReady     bool
	queuedSnaps  map[uint64]struct{}
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

type storeFsm struct {
	id        uint64
	stopped   bool
	startTime *time.Time
	receiver  <-chan message.Msg
	ticker    *ticker
}

func newStoreFsm(cfg *config.Config) (chan<- message.Msg, *storeFsm) {
	ch := make(chan message.Msg, 40960)
	fsm := &storeFsm{
		receiver: (<-chan message.Msg)(ch),
		ticker:   newStoreTicker(cfg),
	}
	return (chan<- message.Msg)(ch), fsm
}

type storeMsgHandler struct {
	*storeFsm
	ctx *StoreContext
}

func newStoreFsmDelegate(store *storeFsm, ctx *StoreContext) *storeMsgHandler {
	return &storeMsgHandler{storeFsm: store, ctx: ctx}
}

func (d *storeMsgHandler) onTick(tick StoreTick) {
	switch tick {
	case StoreTickPdStoreHeartbeat:
		d.onPDStoreHearbeatTick()
	case StoreTickSnapGC:
		d.onSnapMgrGC()
	}
}

func (d *storeMsgHandler) handleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeStoreRaftMessage:
		if err := d.onRaftMessage(msg.Data.(*rspb.RaftMessage)); err != nil {
			log.Errorf("handle raft message failed storeID %d, %v", d.id, err)
		}
	case message.MsgTypeStoreTick:
		d.onTick(msg.Data.(StoreTick))
	case message.MsgTypeStoreStart:
		d.start(msg.Data.(*metapb.Store))
	}
}

func (d *storeMsgHandler) start(store *metapb.Store) {
	if d.startTime != nil {
		panic(fmt.Sprintf("store %d unable to start again %s", d.id, store))
	}
	d.id = store.Id
	now := time.Now()
	d.startTime = &now
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
	d.ticker.scheduleStore(StoreTickSnapGC)
}

/// Checks if the message is targeting a stale peer.
///
/// Returns true means the message can be dropped silently.
func (d *storeMsgHandler) checkMsg(msg *rspb.RaftMessage) (bool, error) {
	regionID := msg.GetRegionId()
	fromEpoch := msg.GetRegionEpoch()
	msgType := msg.Message.MsgType
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.StoreId

	// Check if the target is tombstone,
	stateKey := meta.RegionStateKey(regionID)
	localState := new(rspb.RegionLocalState)
	err := engine_util.GetMsg(d.ctx.engine.Kv, stateKey, localState)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	if localState.State != rspb.PeerState_Tombstone {
		// Maybe split, but not registered yet.
		if util.IsFirstVoteMessage(msg.Message) {
			d.ctx.storeMetaLock.Lock()
			defer d.ctx.storeMetaLock.Unlock()
			meta := d.ctx.storeMeta
			// Last check on whether target peer is created, otherwise, the
			// vote message will never be comsumed.
			if _, ok := meta.regions[regionID]; ok {
				return false, nil
			}
			meta.pendingVotes = append(meta.pendingVotes, msg)
			log.Infof("region %d doesn't exist yet, wait for it to be split.", regionID)
			return true, nil
		}
		return false, errors.Errorf("region %d not exists but not tombstone: %s", regionID, localState)
	}
	log.Debugf("region %d in tombstone state: %s", regionID, localState)
	region := localState.Region
	regionEpoch := region.RegionEpoch
	// The region in this peer is already destroyed
	if util.IsEpochStale(fromEpoch, regionEpoch) {
		log.Infof("tombstone peer receives a stale message. region_id:%d, from_region_epoch:%s, current_region_epoch:%s, msg_type:%s",
			regionID, fromEpoch, regionEpoch, msgType)
		notExist := util.FindPeer(region, fromStoreID) == nil
		handleStaleMsg(d.ctx.trans, msg, regionEpoch, isVoteMsg && notExist)
		return true, nil
	}
	if fromEpoch.ConfVer == regionEpoch.ConfVer {
		return false, errors.Errorf("tombstone peer [epoch: %s] received an invalid message %s, ignore it",
			regionEpoch, msgType)
	}
	return false, nil
}

func (d *storeMsgHandler) onRaftMessage(msg *rspb.RaftMessage) error {
	regionID := msg.RegionId
	if err := d.ctx.router.send(regionID, message.Msg{Type: message.MsgTypeRaftMessage, Data: msg}); err == nil {
		return nil
	}
	log.Debugf("handle raft message. from_peer:%d, to_peer:%d, store:%d, region:%d, msg:%+v",
		msg.FromPeer.Id, msg.ToPeer.Id, d.storeFsm.id, regionID, msg.Message)
	if msg.ToPeer.StoreId != d.ctx.store.Id {
		log.Warnf("store not match, ignore it. store_id:%d, to_store_id:%d, region_id:%d",
			d.ctx.store.Id, msg.ToPeer.StoreId, regionID)
		return nil
	}

	if msg.RegionEpoch == nil {
		log.Errorf("missing region epoch in raft message, ignore it. region_id:%d", regionID)
		return nil
	}
	if msg.IsTombstone {
		// Target tombstone peer doesn't exist, so ignore it.
		return nil
	}
	ok, err := d.checkMsg(msg)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	created, err := d.maybeCreatePeer(regionID, msg)
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	_ = d.ctx.router.send(regionID, message.Msg{Type: message.MsgTypeRaftMessage, Data: msg})
	return nil
}

/// If target peer doesn't exist, create it.
///
/// return false to indicate that target peer is in invalid state or
/// doesn't exist and can't be created.
func (d *storeMsgHandler) maybeCreatePeer(regionID uint64, msg *rspb.RaftMessage) (bool, error) {
	// we may encounter a message with larger peer id, which means
	// current peer is stale, then we should remove current peer
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
	}()
	meta := d.ctx.storeMeta
	if _, ok := meta.regions[regionID]; ok {
		return true, nil
	}
	if !util.IsInitialMsg(msg.Message) {
		log.Debugf("target peer %s doesn't exist", msg.ToPeer)
		return false, nil
	}

	it := meta.regionRanges.NewIterator()
	it.Seek(msg.StartKey)
	if it.Valid() && bytes.Equal(msg.StartKey, it.Key()) {
		it.Next()
	}
	for ; it.Valid(); it.Next() {
		regionID := util.RegionIDFromBytes(it.Value())
		existRegion := meta.regions[regionID]
		if bytes.Compare(existRegion.StartKey, msg.EndKey) >= 0 {
			break
		}
		log.Debugf("msg %s is overlapped with exist region %s", msg, existRegion)
		if util.IsFirstVoteMessage(msg.Message) {
			meta.pendingVotes = append(meta.pendingVotes, msg)
		}
		return false, nil
	}

	peer, err := replicatePeerFsm(
		d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, regionID, msg.ToPeer)
	if err != nil {
		return false, err
	}
	// following snapshot may overlap, should insert into region_ranges after
	// snapshot is applied.
	meta.regions[regionID] = peer.peer.Region()
	d.ctx.router.register(peer)
	_ = d.ctx.router.send(regionID, message.Msg{Type: message.MsgTypeStart})
	return true, nil
}

func (d *storeMsgHandler) storeHeartbeatPD() {
	stats := new(pdpb.StoreStats)
	stats.StoreId = d.ctx.store.Id
	d.ctx.storeMetaLock.RLock()
	stats.RegionCount = uint32(len(d.ctx.storeMeta.regions))
	d.ctx.storeMetaLock.RUnlock()
	storeInfo := &runner.PdStoreHeartbeatTask{
		Stats:  stats,
		Engine: d.ctx.engine.Kv,
		Path:   d.ctx.engine.KvPath,
	}
	d.ctx.pdTaskSender <- worker.Task{Tp: worker.TaskTypePDStoreHeartbeat, Data: storeInfo}
}

func (d *storeMsgHandler) onPDStoreHearbeatTick() {
	d.storeHeartbeatPD()
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
}

func (d *storeMsgHandler) handleSnapMgrGC() error {
	mgr := d.ctx.snapMgr
	snapKeys, err := mgr.ListIdleSnap()
	if err != nil {
		return err
	}
	if len(snapKeys) == 0 {
		return nil
	}
	var lastRegionID uint64
	var keys []snap.SnapKeyWithSending
	for _, pair := range snapKeys {
		key := pair.SnapKey
		if lastRegionID == key.RegionID {
			keys = append(keys, pair)
			continue
		}
		if len(keys) > 0 {
			err = d.scheduleGCSnap(lastRegionID, keys)
			if err != nil {
				return err
			}
			keys = nil
		}
		lastRegionID = key.RegionID
		keys = append(keys, pair)
	}
	if len(keys) > 0 {
		return d.scheduleGCSnap(lastRegionID, keys)
	}
	return nil
}

func (d *storeMsgHandler) scheduleGCSnap(regionID uint64, keys []snap.SnapKeyWithSending) error {
	gcSnap := message.Msg{Type: message.MsgTypeGcSnap, Data: &MsgGCSnap{Snaps: keys}}
	if d.ctx.router.send(regionID, gcSnap) != nil {
		// The snapshot exists because MsgAppend has been rejected. So the
		// peer must have been exist. But now it's disconnected, so the peer
		// has to be destroyed instead of being created.
		log.Infof("region %d is disconnected, remove snaps %v", regionID, keys)
		for _, pair := range keys {
			key := pair.SnapKey
			isSending := pair.IsSending
			var snapshot snap.Snapshot
			var err error
			if isSending {
				snapshot, err = d.ctx.snapMgr.GetSnapshotForSending(key)
			} else {
				snapshot, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
			}
			if err != nil {
				return err
			}
			d.ctx.snapMgr.DeleteSnapshot(key, snapshot, false)
		}
	}
	return nil
}

func (d *storeMsgHandler) onSnapMgrGC() {
	if err := d.handleSnapMgrGC(); err != nil {
		log.Errorf("handle snap GC failed store_id %d, err %s", d.storeFsm.id, err)
	}
	d.ticker.scheduleStore(StoreTickSnapGC)
}
