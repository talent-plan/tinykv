package raftstore

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
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

func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionRanges:        lockstore.NewMemStore(32 * 1024),
		regions:             map[uint64]*metapb.Region{},
		pendingMergeTargets: map[uint64]map[uint64]*metapb.RegionEpoch{},
		targetsMap:          map[uint64]uint64{},
		mergeLocks:          map[uint64]*mergeLock{},
	}
}

func (m *storeMeta) setRegion(region *metapb.Region, peer *Peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

type mergeLock struct {
}

type PollContext struct {
	cfg                  *Config
	engine               *Engines
	applyMsgs            *applyMsgs
	needFlushTrans       bool
	ReadyRes             []ReadyICPair
	kvWB                 *WriteBatch
	raftWB               *WriteBatch
	syncLog              bool
	storeMeta            *storeMeta
	storeMetaLock        *sync.RWMutex
	snapMgr              *SnapManager
	pendingCount         int
	hasReady             bool
	router               *router
	tickDriverCh         chan<- uint64
	trans                Transport
	queuedSnaps          map[uint64]struct{}
	pdScheduler          chan<- task
	raftLogGCScheduler   chan<- task
	store                *metapb.Store
	regionScheduler      chan<- task
	splitCheckScheduler  chan<- task
	computeHashScheduler chan<- task
	cleanUpSSTScheduler  chan<- task
	compactScheduler     chan<- task
	applyingSnapCount    *uint64
	isBusy               bool
	pdClient             pd.Client
	globalStats          *storeStats
	localStats           *storeStats
	peerEventObserver    PeerEventObserver
}

type storeStats struct {
	engineTotalBytesWritten uint64
	engineTotalKeysWritten  uint64
	isBusy                  uint64
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

func (pc *PollContext) flushLocalStats() {
	if pc.localStats.engineTotalBytesWritten > 0 {
		atomic.AddUint64(&pc.globalStats.engineTotalBytesWritten, pc.localStats.engineTotalBytesWritten)
		pc.localStats.engineTotalBytesWritten = 0
	}
	if pc.localStats.engineTotalKeysWritten > 0 {
		atomic.AddUint64(&pc.globalStats.engineTotalKeysWritten, pc.localStats.engineTotalKeysWritten)
		pc.localStats.engineTotalKeysWritten = 0
	}
	if pc.localStats.isBusy > 0 {
		atomic.StoreUint64(&pc.globalStats.isBusy, pc.localStats.isBusy)
		pc.localStats.isBusy = 0
	}
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
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
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

type storeFsm struct {
	id                   uint64
	lastCompactCheckKey  []byte
	stopped              bool
	startTime            *time.Time
	consistencyCheckTime map[uint64]time.Time
	receiver             <-chan Msg
	ticker               *ticker
}

func newStoreFsm(cfg *Config) (chan<- Msg, *storeFsm) {
	ch := make(chan Msg, cfg.NotifyCapacity)
	fsm := &storeFsm{
		consistencyCheckTime: map[uint64]time.Time{},
		receiver:             (<-chan Msg)(ch),
		ticker:               newStoreTicker(cfg),
	}
	return (chan<- Msg)(ch), fsm
}

type storeFsmDelegate struct {
	*storeFsm
	ctx *PollContext
}

func newStoreFsmDelegate(store *storeFsm, ctx *PollContext) *storeFsmDelegate {
	return &storeFsmDelegate{storeFsm: store, ctx: ctx}
}

func (d *storeFsmDelegate) onTick(tick StoreTick) {
	switch tick {
	case StoreTickCompactCheck:
		d.onCompactCheckTick()
	case StoreTickPdStoreHeartbeat:
		d.onPDStoreHearbeatTick()
	case StoreTickSnapGC:
		d.onSnapMgrGC()
	case StoreTickConsistencyCheck:
		d.onComputeHashTick()
	}
}

func (d *storeFsmDelegate) handleMessages(msgs []Msg) {
	for _, msg := range msgs {
		switch msg.Type {
		case MsgTypeStoreRaftMessage:
			if err := d.onRaftMessage(msg.Data.(*rspb.RaftMessage)); err != nil {
				log.Errorf("handle raft message failed storeID %d, %v", d.id, err)
			}
		case MsgTypeStoreSnapshotStats:
			d.storeHeartbeatPD()
		case MsgTypeStoreClearRegionSizeInRange:
			data := msg.Data.(*MsgStoreClearRegionSizeInRange)
			d.clearRegionSizeInRange(data.StartKey, data.EndKey)
		case MsgTypeStoreCompactedEvent:
			d.onCompactionFinished(msg.Data.(*rocksdb.CompactedEvent))
		case MsgTypeStoreTick:
			d.onTick(msg.Data.(StoreTick))
		case MsgTypeStoreStart:
			d.start(msg.Data.(*metapb.Store))
		}
	}
}

func (d *storeFsmDelegate) start(store *metapb.Store) {
	if d.startTime != nil {
		panic(fmt.Sprintf("store %d unable to start again %s", d.id, store))
	}
	d.id = store.Id
	now := time.Now()
	d.startTime = &now
	d.ticker.scheduleStore(StoreTickCompactCheck)
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
	d.ticker.scheduleStore(StoreTickSnapGC)
	d.ticker.scheduleStore(StoreTickConsistencyCheck)
}

type raftPollerBuilder struct {
	cfg                  *Config
	store                *metapb.Store
	pdScheduler          chan<- task
	resolverScheduler    chan<- task
	snapScheduler        chan<- task
	raftLogGCScheduler   chan<- task
	computeHashScheduler chan<- task
	splitCheckScheduler  chan<- task
	regionScheduler      chan<- task
	applyMsgs            *applyMsgs
	router               *router
	compactScheduler     chan<- task
	storeMetaLock        *sync.RWMutex
	storeMeta            *storeMeta
	snapMgr              *SnapManager
	trans                Transport
	pdClient             pd.Client
	engines              *Engines
	applyingSnapCount    *uint64
	tickDriverCh         chan uint64
	peerEventObserver    PeerEventObserver
}

/// init Initialize this store. It scans the db engine, loads all regions
/// and their peers from it, and schedules snapshot worker if necessary.
/// WARN: This store should not be used before initialized.
func (b *raftPollerBuilder) init() ([]*peerFsm, error) {
	// Scan region meta to get saved regions.
	startKey := RegionMetaMinKey
	endKey := RegionMetaMaxKey
	kvEngine := b.engines.kv.DB
	storeID := b.store.Id

	var totalCount, tombStoneCount, applyingCount int
	var regionPeers []*peerFsm

	t := time.Now()
	kvWB := new(WriteBatch)
	raftWB := new(WriteBatch)
	var applyingRegions []*metapb.Region
	var mergingCount int
	b.storeMetaLock.Lock()
	defer b.storeMetaLock.Unlock()
	meta := b.storeMeta
	err := kvEngine.View(func(txn *badger.Txn) error {
		it := dbreader.NewIterator(txn, false, startKey, endKey)
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
				b.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}
			if localState.State == rspb.PeerState_Applying {
				// in case of restart happen when we just write region state to Applying,
				// but not write raft_local_state to raft rocksdb in time.
				applyingCount++
				applyingRegions = append(applyingRegions, region)
				continue
			}

			peer, err := createPeerFsm(storeID, b.cfg, b.regionScheduler, b.engines, region)
			if err != nil {
				return err
			}
			b.peerEventObserver.OnPeerCreate(peer.peer.getEventContext(), region)
			if localState.State == rspb.PeerState_Merging {
				log.Infof("region %d is merging", regionID)
				mergingCount++
				peer.setPendingMergeState(localState.MergeState)
			}
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
	if kvWB.size > 0 {
		kvWB.MustWriteToKV(b.engines.kv)
	}
	if raftWB.size > 0 {
		raftWB.MustWriteToRaft(b.engines.raft)
	}

	// schedule applying snapshot after raft write batch were written.
	for _, region := range applyingRegions {
		log.Infof("region %d is applying snapshot", region.Id)
		peer, err := createPeerFsm(storeID, b.cfg, b.regionScheduler, b.engines, region)
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

func (b *raftPollerBuilder) clearStaleMeta(kvWB, raftWB *WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftKey := RaftStateKey(region.Id)
	raftState := raftState{}
	val, err := getValue(b.engines.raft, raftKey)
	if err != nil {
		// it has been cleaned up.
		return
	}
	raftState.Unmarshal(val)
	err = ClearMeta(b.engines, kvWB, raftWB, region.Id, raftState.lastIndex)
	if err != nil {
		panic(err)
	}
	key := RegionStateKey(region.Id)
	if err := kvWB.SetMsg(key, originState); err != nil {
		panic(err)
	}
}

func (b *raftPollerBuilder) build() *PollContext {
	return &PollContext{
		cfg:                  b.cfg,
		store:                b.store,
		pdScheduler:          b.pdScheduler,
		raftLogGCScheduler:   b.raftLogGCScheduler,
		computeHashScheduler: b.computeHashScheduler,
		splitCheckScheduler:  b.splitCheckScheduler,
		regionScheduler:      b.regionScheduler,
		applyMsgs:            b.applyMsgs,
		router:               b.router,
		compactScheduler:     b.compactScheduler,
		storeMeta:            b.storeMeta,
		storeMetaLock:        b.storeMetaLock,
		snapMgr:              b.snapMgr,
		applyingSnapCount:    b.applyingSnapCount,
		trans:                b.trans,
		queuedSnaps:          make(map[uint64]struct{}),
		pdClient:             b.pdClient,
		engine:               b.engines,
		kvWB:                 new(WriteBatch),
		raftWB:               new(WriteBatch),
		globalStats:          new(storeStats),
		localStats:           new(storeStats),
		tickDriverCh:         b.tickDriverCh,
		peerEventObserver:    b.peerEventObserver,
	}
}

type workers struct {
	pdWorker          *worker
	raftLogGCWorker   *worker
	computeHashWorker *worker
	splitCheckWorker  *worker
	regionWorker      *worker
	compactWorker     *worker
	wg                *sync.WaitGroup
}

type raftBatchSystem struct {
	router     *router
	workers    *workers
	tickDriver *tickDriver
	closeCh    chan struct{}
	wg         *sync.WaitGroup
}

func (bs *raftBatchSystem) start(
	meta *metapb.Store,
	cfg *Config,
	engines *Engines,
	trans Transport,
	pdClinet pd.Client,
	snapMgr *SnapManager,
	pdWorker *worker,
	observer PeerEventObserver) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	workers := &workers{
		splitCheckWorker:  newWorker("split-check", wg),
		regionWorker:      newWorker("snapshot-worker", wg),
		raftLogGCWorker:   newWorker("raft-gc-worker", wg),
		compactWorker:     newWorker("compact-worker", wg),
		pdWorker:          pdWorker,
		computeHashWorker: newWorker("compute-hash", wg),
		wg:                wg,
	}
	builder := &raftPollerBuilder{
		cfg:                  cfg,
		store:                meta,
		engines:              engines,
		router:               bs.router,
		splitCheckScheduler:  workers.splitCheckWorker.scheduler,
		regionScheduler:      workers.regionWorker.scheduler,
		raftLogGCScheduler:   workers.raftLogGCWorker.scheduler,
		compactScheduler:     workers.compactWorker.scheduler,
		pdScheduler:          workers.pdWorker.scheduler,
		computeHashScheduler: workers.computeHashWorker.scheduler,
		trans:                trans,
		pdClient:             pdClinet,
		snapMgr:              snapMgr,
		storeMetaLock:        new(sync.RWMutex),
		storeMeta:            newStoreMeta(),
		applyingSnapCount:    new(uint64),
		tickDriverCh:         bs.tickDriver.newRegionCh,
		peerEventObserver:    observer,
	}
	regionPeers, err := builder.init()
	if err != nil {
		return err
	}
	return bs.startSystem(workers, regionPeers, builder)
}

func (bs *raftBatchSystem) startSystem(
	workers *workers, regionPeers []*peerFsm, builder *raftPollerBuilder) error {
	snapMgr := builder.snapMgr
	err := snapMgr.init()
	if err != nil {
		return err
	}
	router := bs.router
	for _, peer := range regionPeers {
		router.register(peer)
	}
	balancer := &balancer{
		router: router,
	}
	for i := 0; i < builder.cfg.RaftWorkerCnt; i++ {
		rw := newRaftWorker(builder, router.workerSenders[i], router)
		balancer.workers = append(balancer.workers, rw)
		bs.wg.Add(1)
		go rw.run(bs.closeCh, bs.wg)
	}
	sw := &storeWorker{
		store: newStoreFsmDelegate(router.storeFsm, builder.build()),
	}
	bs.wg.Add(1)
	go sw.run(bs.closeCh, bs.wg)
	bs.wg.Add(1)
	go balancer.run(bs.closeCh, bs.wg)
	router.sendStore(Msg{Type: MsgTypeStoreStart, Data: builder.store})
	for i := 0; i < len(regionPeers); i++ {
		regionID := regionPeers[i].peer.regionId
		_ = router.send(regionID, Msg{RegionID: regionID, Type: MsgTypeStart})
	}
	engines := builder.engines
	cfg := builder.cfg
	workers.splitCheckWorker.start(newSplitCheckRunner(engines.kv.DB, router, cfg.splitCheck))
	workers.regionWorker.start(newRegionRunner(engines, snapMgr, cfg.SnapApplyBatchSize, cfg.CleanStalePeerDelay))
	workers.raftLogGCWorker.start(&raftLogGCRunner{})
	workers.compactWorker.start(&compactRunner{engine: engines.kv.DB})
	pdRunner := newPDRunner(builder.store.Id, builder.pdClient, bs.router, engines.kv.DB, workers.pdWorker.scheduler)
	workers.pdWorker.start(pdRunner)
	workers.computeHashWorker.start(&computeHashRunner{router: bs.router})
	bs.workers = workers
	bs.wg.Add(1)
	go bs.tickDriver.run(bs.closeCh, bs.wg) // TODO: temp workaround.
	return nil
}

func (bs *raftBatchSystem) shutDown() {
	if bs.workers == nil {
		return
	}
	close(bs.closeCh)
	bs.wg.Wait()
	workers := bs.workers
	bs.workers = nil
	stopTask := task{tp: taskTypeStop}
	workers.splitCheckWorker.scheduler <- stopTask
	workers.regionWorker.scheduler <- stopTask
	workers.raftLogGCWorker.scheduler <- stopTask
	workers.computeHashWorker.scheduler <- stopTask
	workers.pdWorker.scheduler <- stopTask
	workers.compactWorker.scheduler <- stopTask
	workers.wg.Wait()
}

func createRaftBatchSystem(cfg *Config) (*router, *raftBatchSystem) {
	storeSender, storeFsm := newStoreFsm(cfg)
	router := newRouter(cfg.RaftWorkerCnt, storeSender, storeFsm)
	raftBatchSystem := &raftBatchSystem{
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
func (d *storeFsmDelegate) checkMsg(msg *rspb.RaftMessage) (bool, error) {
	regionID := msg.GetRegionId()
	fromEpoch := msg.GetRegionEpoch()
	msgType := msg.Message.MsgType
	isVoteMsg := isVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.StoreId

	// Check if the target is tombstone,
	stateKey := RegionStateKey(regionID)
	localState := new(rspb.RegionLocalState)
	err := getMsg(d.ctx.engine.kv.DB, stateKey, localState)
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
	if localState.MergeState != nil {
		// TODO: Merge
		return true, nil
	}
	// The region in this peer is already destroyed
	if IsEpochStale(fromEpoch, regionEpoch) {
		log.Infof("tombstone peer receives a stale message. region_id:%d, from_region_epoch:%s, current_region_epoch:%s, msg_type:%s",
			regionID, fromEpoch, regionEpoch, msgType)
		notExist := findPeer(region, fromStoreID) == nil
		d.ctx.handleStaleMsg(msg, regionEpoch, isVoteMsg && notExist, nil)
		return true, nil
	}
	if fromEpoch.ConfVer == regionEpoch.ConfVer {
		return false, errors.Errorf("tombstone peer [epoch: %s] received an invalid message %s, ignore it",
			regionEpoch, msgType)
	}
	return false, nil
}

func (d *storeFsmDelegate) onRaftMessage(msg *rspb.RaftMessage) error {
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
	if msg.IsTombstone || msg.MergeTarget != nil {
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
func (d *storeFsmDelegate) maybeCreatePeer(regionID uint64, msg *rspb.RaftMessage) (bool, error) {
	var regionsToDestroy []uint64
	// we may encounter a message with larger peer id, which means
	// current peer is stale, then we should remove current peer
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// send message out of store meta lock, to avoid dead lock.
		destroyRegions(d.ctx.router, regionsToDestroy, msg.ToPeer)
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

		// Make sure the range of region from msg is covered by existing regions.
		// If so, means that the region may be generated by some kinds of split
		// and merge by catching logs. So there is no need to accept a snapshot.
		if !isRangeCovered(meta, msg.StartKey, msg.EndKey) {
			if maybeDestroySource(meta, regionID, existRegion.Id, msg.RegionEpoch) {
				regionsToDestroy = append(regionsToDestroy, existRegion.Id)
				continue
			}
		}
		regionsToDestroy = nil
		return false, nil
	}

	// New created peers should know it's learner or not.
	peer, err := replicatePeerFsm(
		d.ctx.store.Id, d.ctx.cfg, d.ctx.regionScheduler, d.ctx.engine, regionID, msg.ToPeer)
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

func destroyRegions(router *router, regionsToDestroy []uint64, toPeer *metapb.Peer) {
	for _, id := range regionsToDestroy {
		_ = router.send(id, Msg{Type: MsgTypeMergeResult, Data: &MsgMergeResult{
			TargetPeer: toPeer,
			Stale:      true,
		}})
	}
}

func (d *storeFsmDelegate) onCompactionFinished(event *rocksdb.CompactedEvent) {
	// TODO: not supported.
}

func (d *storeFsmDelegate) onCompactCheckTick() {
	// TODO: not supported.
}

func (d *storeFsmDelegate) storeHeartbeatPD() {
	stats := new(pdpb.StoreStats)
	stats.UsedSize = d.ctx.snapMgr.GetTotalSnapSize()
	stats.StoreId = d.ctx.store.Id
	d.ctx.storeMetaLock.RLock()
	stats.RegionCount = uint32(len(d.ctx.storeMeta.regions))
	d.ctx.storeMetaLock.RUnlock()
	snapStats := d.ctx.snapMgr.Stats()
	stats.SendingSnapCount = uint32(snapStats.SendingCount)
	stats.ReceivingSnapCount = uint32(snapStats.ReceivingCount)
	stats.ApplyingSnapCount = uint32(atomic.LoadUint64(d.ctx.applyingSnapCount))
	stats.StartTime = uint32(d.startTime.Second())
	globalStats := d.ctx.globalStats
	stats.BytesWritten = atomic.SwapUint64(&globalStats.engineTotalBytesWritten, 0)
	stats.KeysWritten = atomic.SwapUint64(&globalStats.engineTotalKeysWritten, 0)
	stats.IsBusy = atomic.SwapUint64(&globalStats.isBusy, 0) > 0
	storeInfo := &pdStoreHeartbeatTask{
		stats:    stats,
		engine:   d.ctx.engine.kv.DB,
		capacity: d.ctx.cfg.Capacity,
		path:     d.ctx.engine.kvPath,
	}
	d.ctx.pdScheduler <- task{tp: taskTypePDStoreHeartbeat, data: storeInfo}
}

func (d *storeFsmDelegate) onPDStoreHearbeatTick() {
	d.storeHeartbeatPD()
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
}

func (d *storeFsmDelegate) handleSnapMgrGC() error {
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

func (d *storeFsmDelegate) scheduleGCSnap(regionID uint64, keys []SnapKeyWithSending) error {
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

func (d *storeFsmDelegate) onSnapMgrGC() {
	if err := d.handleSnapMgrGC(); err != nil {
		log.Errorf("handle snap GC failed store_id %d, err %s", d.storeFsm.id, err)
	}
	d.ticker.scheduleStore(StoreTickSnapGC)
}

func (d *storeFsmDelegate) onComputeHashTick() {
	d.ticker.scheduleStore(StoreTickConsistencyCheck)
	if len(d.ctx.computeHashScheduler) > 0 {
		return
	}
	targetRegion := d.findTargetRegionForComputeHash()
	if targetRegion == nil {
		return
	}
	peer := findPeer(targetRegion, d.ctx.store.Id)
	if peer == nil {
		return
	}
	log.Infof("schedule consistency check for region %d, store %d", targetRegion.Id, peer.StoreId)
	d.storeFsm.consistencyCheckTime[targetRegion.Id] = time.Now()
	request := newAdminRequest(targetRegion.Id, peer)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_ComputeHash,
	}
	cmd := &MsgRaftCmd{
		Request: request,
	}
	_ = d.ctx.router.sendRaftCommand(cmd)
}

func (d *storeFsmDelegate) findTargetRegionForComputeHash() *metapb.Region {
	oldest := time.Now()
	var targetRegion *metapb.Region
	d.ctx.storeMetaLock.RLock()
	defer d.ctx.storeMetaLock.RUnlock()
	meta := d.ctx.storeMeta
	for regionID, region := range meta.regions {
		if t, ok := d.storeFsm.consistencyCheckTime[regionID]; ok {
			if t.Before(oldest) {
				oldest = t
				targetRegion = region
			}
		} else {
			targetRegion = region
			break
		}
	}
	return targetRegion
}

func (d *storeFsmDelegate) clearRegionSizeInRange(startKey, endKey []byte) {
	regions := d.findRegionsInRange(startKey, endKey)
	for _, region := range regions {
		_ = d.ctx.router.send(region.Id, NewPeerMsg(MsgTypeClearRegionSize, region.Id, nil))
	}
}

func (d *storeFsmDelegate) findRegionsInRange(startKey, endKey []byte) []*metapb.Region {
	d.ctx.storeMetaLock.RLock()
	defer d.ctx.storeMetaLock.RUnlock()
	meta := d.ctx.storeMeta
	it := meta.regionRanges.NewIterator()
	it.Seek(startKey)
	if it.Valid() && bytes.Equal(it.Key(), startKey) {
		it.Next()
	}
	var regions []*metapb.Region
	for ; it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), endKey) > 0 {
			break
		}
		regionID := regionIDFromBytes(it.Value())
		regions = append(regions, meta.regions[regionID])
	}
	return regions
}

type regionIDDeclinedBytesPair struct {
	regionID      uint64
	declinedBytes uint64
}

func calcRegionDeclinedBytes(event *rocksdb.CompactedEvent,
	regionRanges *lockstore.MemStore, bytesThreshold uint64) []regionIDDeclinedBytesPair {
	return nil // TODO: not supported.
}

func isRangeCovered(meta *storeMeta, start, end []byte) bool {
	it := meta.regionRanges.NewIterator()
	it.Seek(start)
	if it.Valid() && bytes.Equal(it.Key(), start) {
		it.Next()
	}
	for ; it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), end) > 0 {
			break
		}
		regionID := regionIDFromBytes(it.Value())
		region := meta.regions[regionID]
		// find a missing range
		if bytes.Compare(start, region.StartKey) < 0 {
			return false
		}
		if bytes.Compare(region.EndKey, end) >= 0 {
			return true
		}
		start = region.EndKey
	}
	return false
}
