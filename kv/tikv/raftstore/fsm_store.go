package raftstore

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/lockstore"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
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
	snapMgr              *SnapManager
	router               *router
	trans                Transport
	pdTaskSender         chan<- worker.Task
	regionTaskSender     chan<- worker.Task
	raftLogGCTaskSender  chan<- worker.Task
	splitCheckTaskSender chan<- worker.Task
	pdClient             pd.Client
	peerEventObserver    PeerEventObserver
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
	id                  uint64
	lastCompactCheckKey []byte
	stopped             bool
	startTime           *time.Time
	receiver            <-chan Msg
	ticker              *ticker
}

func newStoreFsm(cfg *config.Config) (chan<- Msg, *storeFsm) {
	ch := make(chan Msg, cfg.NotifyCapacity)
	fsm := &storeFsm{
		receiver: (<-chan Msg)(ch),
		ticker:   newStoreTicker(cfg),
	}
	return (chan<- Msg)(ch), fsm
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

func (d *storeMsgHandler) handleMsg(msg Msg) {
	switch msg.Type {
	case MsgTypeStoreRaftMessage:
		if err := d.onRaftMessage(msg.Data.(*rspb.RaftMessage)); err != nil {
			log.Errorf("handle raft message failed storeID %d, %v", d.id, err)
		}
	case MsgTypeStoreSnapshotStats:
		d.storeHeartbeatPD()
	case MsgTypeStoreTick:
		d.onTick(msg.Data.(StoreTick))
	case MsgTypeStoreStart:
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

/// loadPeers loads peers in this store. It scans the db engine, loads all regions
/// and their peers from it, and schedules snapshot worker if necessary.
/// WARN: This store should not be used before initialized.
func (bs *RaftBatchSystem) loadPeers() ([]*peerFsm, error) {
	// Scan region meta to get saved regions.
	startKey := RegionMetaMinKey
	endKey := RegionMetaMaxKey
	ctx := bs.ctx
	kvEngine := ctx.engine.Kv
	storeID := ctx.store.Id

	var totalCount, tombStoneCount, applyingCount int
	var regionPeers []*peerFsm

	t := time.Now()
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	var applyingRegions []*metapb.Region
	var mergingCount int
	ctx.storeMetaLock.Lock()
	defer ctx.storeMetaLock.Unlock()
	meta := ctx.storeMeta
	err := kvEngine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := decodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}
			if localState.State == rspb.PeerState_Applying {
				// in case of restart happen when we just write region state to Applying,
				// but not write raft_local_state to raft rocksdb in time.
				applyingCount++
				applyingRegions = append(applyingRegions, region)
				continue
			}

			peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			ctx.peerEventObserver.OnPeerCreate(peer.peer.getEventContext(), region)
			meta.regionRanges.Insert(region.EndKey, regionIDToBytes(regionID))
			meta.regions[regionID] = region
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kvWB.MustWriteToKV(ctx.engine.Kv)
	raftWB.MustWriteToRaft(ctx.engine.Raft)

	// schedule applying snapshot after raft write batch were written.
	for _, region := range applyingRegions {
		log.Infof("region %d is applying snapshot", region.Id)
		peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
		if err != nil {
			return nil, err
		}
		peer.scheduleApplyingSnapshot()
		meta.regionRanges.Insert(region.EndKey, regionIDToBytes(region.Id))
		meta.regions[region.Id] = region
		regionPeers = append(regionPeers, peer)
	}
	log.Infof("start store %d, region_count %d, tombstone_count %d, applying_count %d, merge_count %d, takes %v",
		storeID, totalCount, tombStoneCount, applyingCount, mergingCount, time.Since(t))
	return regionPeers, nil
}

func (bs *RaftBatchSystem) clearStaleMeta(kvWB, raftWB *engine_util.WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftKey := RaftStateKey(region.Id)
	raftState := raftState{}
	val, err := getValue(bs.ctx.engine.Raft, raftKey)
	if err != nil {
		// it has been cleaned up.
		return
	}
	raftState.Unmarshal(val)
	err = ClearMeta(bs.ctx.engine, kvWB, raftWB, region.Id, raftState.lastIndex)
	if err != nil {
		panic(err)
	}
	key := RegionStateKey(region.Id)
	if err := kvWB.SetMsg(key, originState); err != nil {
		panic(err)
	}
}

type workers struct {
	raftLogGCWorker  *worker.Worker
	pdWorker         *worker.Worker
	splitCheckWorker *worker.Worker
	regionWorker     *worker.Worker
	wg               *sync.WaitGroup
}

type RaftBatchSystem struct {
	ctx        *GlobalContext
	router     *router
	workers    *workers
	tickDriver *tickDriver
	closeCh    chan struct{}
	wg         *sync.WaitGroup
}

func (bs *RaftBatchSystem) start(
	meta *metapb.Store,
	cfg *config.Config,
	engines *engine_util.Engines,
	trans Transport,
	pdClient pd.Client,
	snapMgr *SnapManager,
	pdWorker *worker.Worker,
	observer PeerEventObserver) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	err := snapMgr.init()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker: worker.NewWorker("split-check", wg),
		regionWorker:     worker.NewWorker("snapshot-worker", wg),
		raftLogGCWorker:  worker.NewWorker("raft-gc-worker", wg),
		pdWorker:         pdWorker,
		wg:               wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                  cfg,
		engine:               engines,
		store:                meta,
		storeMeta:            newStoreMeta(),
		storeMetaLock:        new(sync.RWMutex),
		snapMgr:              snapMgr,
		router:               bs.router,
		trans:                trans,
		pdTaskSender:         bs.workers.pdWorker.Sender(),
		regionTaskSender:     bs.workers.regionWorker.Sender(),
		splitCheckTaskSender: bs.workers.splitCheckWorker.Sender(),
		raftLogGCTaskSender:  bs.workers.raftLogGCWorker.Sender(),
		pdClient:             pdClient,
		peerEventObserver:    observer,
		tickDriverSender:     bs.tickDriver.newRegionCh,
	}
	regionPeers, err := bs.loadPeers()
	if err != nil {
		return err
	}

	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	bs.startWorkers(regionPeers)
	return nil
}

func (bs *RaftBatchSystem) startWorkers(peers []*peerFsm) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router
	balancer := &balancer{
		router: router,
	}
	for i := 0; i < ctx.cfg.RaftWorkerCnt; i++ {
		rw := newRaftWorker(ctx, router.workerSenders[i], router)
		balancer.workers = append(balancer.workers, rw)
		bs.wg.Add(1)
		go rw.run(bs.closeCh, bs.wg)
	}
	storeCtx := &StoreContext{GlobalContext: ctx, applyingSnapCount: new(uint64)}
	sw := &storeWorker{
		store: newStoreFsmDelegate(router.storeFsm, storeCtx),
	}
	bs.wg.Add(1)
	go sw.run(bs.closeCh, bs.wg)
	bs.wg.Add(1)
	go balancer.run(bs.closeCh, bs.wg)
	router.sendStore(Msg{Type: MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].peer.regionId
		_ = router.send(regionID, Msg{RegionID: regionID, Type: MsgTypeStart})
	}
	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.Start(newSplitCheckHandler(engines.Kv, router, cfg.SplitCheck))
	workers.regionWorker.Start(newRegionTaskHandler(engines, ctx.snapMgr, cfg.SnapApplyBatchSize, cfg.CleanStalePeerDelay))
	workers.raftLogGCWorker.Start(&raftLogGCTaskHandler{})
	workers.pdWorker.Start(newPDTaskHandler(ctx.store.Id, ctx.pdClient, bs.router))
	bs.wg.Add(1)
	go bs.tickDriver.run(bs.closeCh, bs.wg) // TODO: temp workaround.
}

func (bs *RaftBatchSystem) shutDown() {
	if bs.workers == nil {
		return
	}
	close(bs.closeCh)
	bs.wg.Wait()
	workers := bs.workers
	bs.workers = nil
	stopTask := worker.Task{Tp: worker.TaskTypeStop}
	workers.splitCheckWorker.Sender() <- stopTask
	workers.regionWorker.Sender() <- stopTask
	workers.raftLogGCWorker.Sender() <- stopTask
	workers.pdWorker.Sender() <- stopTask
	workers.wg.Wait()
}

func CreateRaftBatchSystem(cfg *config.Config) (*router, *RaftBatchSystem) {
	storeSender, storeFsm := newStoreFsm(cfg)
	router := newRouter(cfg.RaftWorkerCnt, storeSender, storeFsm)
	raftBatchSystem := &RaftBatchSystem{
		router:     router,
		tickDriver: newTickDriver(cfg.RaftBaseTickInterval, router, storeFsm.ticker),
		closeCh:    make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
	return router, raftBatchSystem
}

/// Checks if the message is targeting a stale peer.
///
/// Returns true means the message can be dropped silently.
func (d *storeMsgHandler) checkMsg(msg *rspb.RaftMessage) (bool, error) {
	regionID := msg.GetRegionId()
	fromEpoch := msg.GetRegionEpoch()
	msgType := msg.Message.MsgType
	isVoteMsg := isVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.StoreId

	// Check if the target is tombstone,
	stateKey := RegionStateKey(regionID)
	localState := new(rspb.RegionLocalState)
	err := getMsg(d.ctx.engine.Kv, stateKey, localState)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	if localState.State != rspb.PeerState_Tombstone {
		// Maybe split, but not registered yet.
		if isFirstVoteMessage(msg.Message) {
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
	if IsEpochStale(fromEpoch, regionEpoch) {
		log.Infof("tombstone peer receives a stale message. region_id:%d, from_region_epoch:%s, current_region_epoch:%s, msg_type:%s",
			regionID, fromEpoch, regionEpoch, msgType)
		notExist := findPeer(region, fromStoreID) == nil
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
	if err := d.ctx.router.send(regionID, Msg{Type: MsgTypeRaftMessage, Data: msg}); err == nil {
		return nil
	}
	log.Debugf("handle raft message. from_peer:%d, to_peer:%d, store:%d, region:%d, msg_type:%s",
		msg.FromPeer.Id, msg.ToPeer.Id, d.storeFsm.id, regionID, msg.Message.MsgType)
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
	_ = d.ctx.router.send(regionID, Msg{Type: MsgTypeRaftMessage, Data: msg})
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
	if !isInitialMsg(msg.Message) {
		log.Debugf("target peer %s doesn't exist", msg.ToPeer)
		return false, nil
	}

	it := meta.regionRanges.NewIterator()
	it.Seek(msg.StartKey)
	if it.Valid() && bytes.Equal(msg.StartKey, it.Key()) {
		it.Next()
	}
	for ; it.Valid(); it.Next() {
		regionID := regionIDFromBytes(it.Value())
		existRegion := meta.regions[regionID]
		if bytes.Compare(existRegion.StartKey, msg.EndKey) >= 0 {
			break
		}
		log.Debugf("msg %s is overlapped with exist region %s", msg, existRegion)
		if isFirstVoteMessage(msg.Message) {
			meta.pendingVotes = append(meta.pendingVotes, msg)
		}
		return false, nil
	}

	// New created peers should know it's learner or not.
	peer, err := replicatePeerFsm(
		d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, regionID, msg.ToPeer)
	if err != nil {
		return false, err
	}
	// following snapshot may overlap, should insert into region_ranges after
	// snapshot is applied.
	meta.regions[regionID] = peer.peer.Region()
	d.ctx.router.register(peer)
	_ = d.ctx.router.send(regionID, Msg{Type: MsgTypeStart})
	d.ctx.peerEventObserver.OnPeerCreate(peer.peer.getEventContext(), peer.peer.Region())
	return true, nil
}

func (d *storeMsgHandler) storeHeartbeatPD() {
	stats := new(pdpb.StoreStats)
	stats.StoreId = d.ctx.store.Id
	d.ctx.storeMetaLock.RLock()
	stats.RegionCount = uint32(len(d.ctx.storeMeta.regions))
	d.ctx.storeMetaLock.RUnlock()
	storeInfo := &pdStoreHeartbeatTask{
		stats:    stats,
		engine:   d.ctx.engine.Kv,
		capacity: d.ctx.cfg.Capacity,
		path:     d.ctx.engine.KvPath,
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
	var keys []SnapKeyWithSending
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

func (d *storeMsgHandler) scheduleGCSnap(regionID uint64, keys []SnapKeyWithSending) error {
	gcSnap := Msg{Type: MsgTypeGcSnap, Data: &MsgGCSnap{Snaps: keys}}
	if d.ctx.router.send(regionID, gcSnap) != nil {
		// The snapshot exists because MsgAppend has been rejected. So the
		// peer must have been exist. But now it's disconnected, so the peer
		// has to be destroyed instead of being created.
		log.Infof("region %d is disconnected, remove snaps %v", regionID, keys)
		for _, pair := range keys {
			key := pair.SnapKey
			isSending := pair.IsSending
			var snap Snapshot
			var err error
			if isSending {
				snap, err = d.ctx.snapMgr.GetSnapshotForSending(key)
			} else {
				snap, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
			}
			if err != nil {
				return err
			}
			d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
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
