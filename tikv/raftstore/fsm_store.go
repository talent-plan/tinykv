package raftstore

import (
	"bytes"
	"fmt"
	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/zhangjinpeng1987/raft"
	"sync"
	"time"
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

func (m *storeMeta) setRegion(host *CoprocessorHost, region *metapb.Region, peer *Peer) {
	m.regions[region.Id] = CloneRegion(region)
	peer.SetRegion(host, region)
}

type mergeLock struct {
}

type PollContext struct {
	cfg                  *Config
	coprocessorHost      *CoprocessorHost
	engine               *Engines
	dbBundle             *DBBundle
	applyRouter          *applyRouter
	needFlushTrans       bool
	ReadyRes             []ReadyICPair
	kvWB                 *WriteBatch
	raftWB               *WriteBatch
	syncLog              bool
	storeMeta            *storeMeta
	storeMetaLock        *sync.Mutex
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
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

func (r *router) sendRaftMessage(msg *rspb.RaftMessage) {
	mb := r.mailbox(msg.RegionId)
	if mb != nil {
		mb.send(Msg{Type: MsgTypeRaftMessage, Data: msg}, r.normalScheduler)
	} else {
		mb.send(Msg{Type: MsgTypeStoreRaftMessage, Data: msg}, r.controlScheduler)
	}
}

func (r *router) sendRaftCommand(cmd *MsgRaftCmd) error {
	regionID := cmd.Request.Header.RegionId
	return r.send(regionID, Msg{Type: MsgTypeRaftCmd, Data: cmd})
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

func (s *storeFsm) isStopped() bool {
	return s.stopped
}

func (s *storeFsm) setMailbox(mb *mailbox) {}

func (s *storeFsm) takeMailbox() *mailbox {
	return nil
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
	case StoreTickCleanupImportSST:
		d.onCleanUpImportSSTTick()
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
		case MsgTypeStoreValidateSSTResult:
			data := msg.Data.(*MsgStoreValidateSSTResult)
			d.onValidateSSTResult(data.InvalidSSTs)
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
	d.ticker.scheduleStore(StoreTickCleanupImportSST)
	d.ticker.scheduleStore(StoreTickCompactCheck)
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
	d.ticker.scheduleStore(StoreTickSnapGC)
	d.ticker.scheduleStore(StoreTickConsistencyCheck)
}

type raftPoller struct {
	tag              string
	storeMsgBuf      []Msg
	peerMsgBuf       []Msg
	pollCtx          *PollContext
	pendingProposals []*regionProposal
	messagesPerTick  int
	startTime        time.Time
}

func (p *raftPoller) handleRaftReady(peers []fsm) {
	if len(p.pendingProposals) > 0 {
		for _, proposal := range p.pendingProposals {
			msg := Msg{Type: MsgTypeApplyProposal, Data: proposal}
			p.pollCtx.applyRouter.scheduleTask(proposal.RegionId, msg)
		}
	}
	kvWB := p.pollCtx.kvWB
	if len(kvWB.entries) > 0 {
		err := kvWB.WriteToDB(p.pollCtx.engine.kv)
		if err != nil {
			panic(err)
		}
		kvWB.Reset()
	}
	raftWB := p.pollCtx.raftWB
	if len(raftWB.entries) > 0 {
		err := raftWB.WriteToDB(p.pollCtx.engine.raft)
		if err != nil {
			panic(err)
		}
		raftWB.Reset()
	}
	readyRes := p.pollCtx.ReadyRes
	if len(readyRes) > 0 {
		batchPos := 0
		for _, pair := range readyRes {
			regionID := pair.IC.RegionID
			for peers[batchPos].(*peerFsm).peer.regionId != regionID {
				batchPos++
			}
			newPeerFsmDelegate(peers[batchPos].(*peerFsm), p.pollCtx).postRaftReadyAppend(&pair.Ready, pair.IC)
		}
	}
	dur := time.Since(p.startTime)
	if !p.pollCtx.isBusy {
		electionTimeout := p.pollCtx.cfg.RaftBaseTickInterval * time.Duration(p.pollCtx.cfg.RaftElectionTimeoutTicks)
		if dur > electionTimeout {
			p.pollCtx.isBusy = true
		}
	}
	return
}

func (p *raftPoller) begin(batchSize int) {
	p.pollCtx.pendingCount = 0
	p.pollCtx.hasReady = false
	p.startTime = time.Now()
	return
}

func (p *raftPoller) handleControl(fsm fsm) (pause bool, chLen int) {
	store := fsm.(*storeFsm)
	numToRecv := p.messagesPerTick - len(p.storeMsgBuf)
	receiverLen := len(store.receiver)
	if numToRecv >= receiverLen {
		numToRecv = receiverLen
		pause = true
	}
	for i := 0; i < numToRecv; i++ {
		p.storeMsgBuf = append(p.storeMsgBuf, <-store.receiver)
	}
	newStoreFsmDelegate(store, p.pollCtx).handleMessages(p.storeMsgBuf)
	p.storeMsgBuf = p.storeMsgBuf[:0]
	return
}

func (p *raftPoller) handleNormal(normal fsm) (pause bool, chLen int) {
	peer := normal.(*peerFsm)
	receiverLen := len(peer.receiver)
	delegate := newPeerFsmDelegate(peer, p.pollCtx)
	if peer.hasPendingMergeApplyResult() {
		if !delegate.resumeHandlePendingApplyResult() {
			return true, receiverLen
		}
	}
	numToRecv := p.messagesPerTick - len(p.storeMsgBuf)
	if numToRecv >= receiverLen {
		numToRecv = receiverLen
		pause = true
	}
	for i := 0; i < numToRecv; i++ {
		p.peerMsgBuf = append(p.peerMsgBuf, <-peer.receiver)
	}
	delegate.handleMsgs(p.peerMsgBuf)
	p.peerMsgBuf = p.peerMsgBuf[:0]
	p.pendingProposals = delegate.collectReady(p.pendingProposals)
	return
}

func (p *raftPoller) end(peers []fsm) {
	if p.pollCtx.hasReady {
		p.handleRaftReady(peers)
	}
	if len(p.pollCtx.queuedSnaps) > 0 {
		p.pollCtx.storeMetaLock.Lock()
		meta := p.pollCtx.storeMeta
		retained := meta.pendingSnapshotRegions[:0]
		for _, region := range meta.pendingSnapshotRegions {
			if _, ok := p.pollCtx.queuedSnaps[region.Id]; !ok {
				retained = append(retained, region)
			}
		}
		meta.pendingSnapshotRegions = retained
		p.pollCtx.storeMetaLock.Unlock()
		p.pollCtx.queuedSnaps = map[uint64]struct{}{}
	}
	return
}

type raftPollerBuilder struct {
	cfg                  *Config
	store                *metapb.Store
	pdScheduler          chan<- task
	raftLogGCScheduler   chan<- task
	computeHashScheduler chan<- task
	splitCheckScheduler  chan<- task
	regionScheduler      chan<- task
	applyRouter          *applyRouter
	router               *router
	compactScheduler     chan<- task
	storeMetaMu          *sync.Mutex
	storeMeta            *storeMeta
	snapMgr              *SnapManager
	coprocessorHost      *CoprocessorHost
	trans                Transport
	pdClient             pd.Client
	engines              *Engines
	applyingSnapCount    *uint64
}

type senderPeerFsmPair struct {
	sender chan<- Msg
	fsm    *peerFsm
}

/// init Initialize this store. It scans the db engine, loads all regions
/// and their peers from it, and schedules snapshot worker if necessary.
/// WARN: This store should not be used before initialized.
func (b *raftPollerBuilder) init() ([]senderPeerFsmPair, error) {
	// Scan region meta to get saved regions.
	startKey := RegionMetaMinKey
	endKey := RegionMetaMaxKey
	kvEngine := b.engines.kv
	storeID := b.store.Id

	var totalCount, tombStoneCount, applyingCount int
	var regionPeers []senderPeerFsmPair

	t := time.Now()
	kvWB := new(WriteBatch)
	raftWB := new(WriteBatch)
	var applyingRegions []*metapb.Region
	var mergingCount int
	b.storeMetaMu.Lock()
	defer b.storeMetaMu.Unlock()
	meta := b.storeMeta
	err := kvEngine.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.StartKey = startKey
		opt.EndKey = endKey
		opt.PrefetchValues = false
		it := txn.NewIterator(opt)
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
				applyingRegions = append(applyingRegions, CloneRegion(region))
				continue
			}

			sender, peer, err := createPeerFsm(storeID, b.cfg, b.regionScheduler, b.engines, region)
			if err != nil {
				return err
			}
			if localState.State == rspb.PeerState_Merging {
				log.Infof("region %d is merging", regionID)
				mergingCount++
				peer.setPendingMergeState(CloneMergeState(localState.MergeState))
			}
			meta.regionRanges.Insert(region.EndKey, regionIDToBytes(regionID))
			meta.regions[regionID] = CloneRegion(region)
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			regionPeers = append(regionPeers, senderPeerFsmPair{sender: sender, fsm: peer})
			b.coprocessorHost.OnRegionChanged(region, RegionChangeEvent_Create, raft.StateFollower)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if kvWB.size > 0 {
		kvWB.MustWriteToDB(b.engines.kv)
	}
	if raftWB.size > 0 {
		raftWB.MustWriteToDB(b.engines.raft)
	}

	// schedule applying snapshot after raft write batch were written.
	for _, region := range applyingRegions {
		log.Infof("region %d is applying snapshot", region.Id)
		sender, peer, err := createPeerFsm(storeID, b.cfg, b.regionScheduler, b.engines, region)
		if err != nil {
			return nil, err
		}
		peer.scheduleApplyingSnapshot()
		meta.regionRanges.Insert(region.EndKey, regionIDToBytes(region.Id))
		meta.regions[region.Id] = CloneRegion(region)
		regionPeers = append(regionPeers, senderPeerFsmPair{sender: sender, fsm: peer})
	}
	log.Infof("start store %d, region_count %d, tombstone_count %d, applying_count %d, merge_count %d, takes %v",
		storeID, totalCount, tombStoneCount, applyingCount, mergingCount, time.Since(t))
	return regionPeers, nil
}

func (b *raftPollerBuilder) clearStaleMeta(kvWB, raftWB *WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftKey := RaftStateKey(region.Id)
	raftState := new(rspb.RaftLocalState)
	err := getMsg(b.engines.raft, raftKey, raftState)
	if err != nil {
		// it has been cleaned up.
		return
	}
	err = ClearMeta(b.engines, kvWB, raftWB, region.Id, raftState)
	if err != nil {
		panic(err)
	}
	key := RegionStateKey(region.Id)
	if err := kvWB.SetMsg(key, originState); err != nil {
		panic(err)
	}
}

func (b *raftPollerBuilder) build() pollHandler {
	ctx := &PollContext{
		cfg:                  b.cfg,
		store:                b.store,
		pdScheduler:          b.pdScheduler,
		raftLogGCScheduler:   b.raftLogGCScheduler,
		computeHashScheduler: b.computeHashScheduler,
		splitCheckScheduler:  b.splitCheckScheduler,
		regionScheduler:      b.regionScheduler,
		applyRouter:          b.applyRouter,
		router:               b.router,
		compactScheduler:     b.compactScheduler,
		storeMeta:            b.storeMeta,
		storeMetaLock:        b.storeMetaMu,
		snapMgr:              b.snapMgr,
		applyingSnapCount:    b.applyingSnapCount,
		coprocessorHost:      b.coprocessorHost,
		trans:                b.trans,
		pdClient:             b.pdClient,
		engine:               b.engines,
		kvWB:                 new(WriteBatch),
		raftWB:               new(WriteBatch),
	}
	return &raftPoller{
		tag:             fmt.Sprintf("[store %d]", ctx.store.Id),
		storeMsgBuf:     make([]Msg, 0, ctx.cfg.MessagesPerTick),
		peerMsgBuf:      make([]Msg, 0, ctx.cfg.MessagesPerTick),
		startTime:       time.Now(),
		messagesPerTick: int(ctx.cfg.MessagesPerTick),
		pollCtx:         ctx,
	}
}

type workers struct {
	pdWorker          *worker
	raftLogGCWorker   *worker
	computeHashWorker *worker
	splitCheckWorker  *worker
	regionWorker      *worker
	compactWorker     *worker
	coprocessorHost   *CoprocessorHost
	wg                *sync.WaitGroup
}

type raftBatchSystem struct {
	system      *batchSystem
	applyRouter *applyRouter
	applySystem *batchSystem
	router      *router
	workers     *workers
}

func (bs *raftBatchSystem) start(
	meta *metapb.Store,
	cfg *Config,
	engines *Engines,
	trans Transport,
	pdClinet pd.Client,
	snapMgr *SnapManager,
	coprocessorHost *CoprocessorHost) error {
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
		pdWorker:          newWorker("pd-worker", wg),
		computeHashWorker: newWorker("compute-hash", wg),
		coprocessorHost:   coprocessorHost,
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
		applyRouter:          bs.applyRouter,
		trans:                trans,
		pdClient:             pdClinet,
		coprocessorHost:      coprocessorHost,
		snapMgr:              snapMgr,
		storeMetaMu:          new(sync.Mutex),
		storeMeta:            newStoreMeta(),
		applyingSnapCount:    new(uint64),
	}
	regionPeers, err := builder.init()
	if err != nil {
		return err
	}
	return bs.startSystem(workers, regionPeers, builder)
}

func (bs *raftBatchSystem) startSystem(
	workers *workers, regionPeers []senderPeerFsmPair, builder *raftPollerBuilder) error {
	snapMgr := builder.snapMgr
	err := snapMgr.init()
	if err != nil {
		return err
	}
	applyPollerBuilder := newApplyPollerBuilder(builder, notifier{router: bs.router}, bs.applyRouter)
	bs.scheduleApplySystem(regionPeers)

	store := builder.store
	tag := fmt.Sprintf("raftstore-%d", store.Id)
	bs.system.start(tag, builder)

	mailboxes := make([]addrMailboxPair, 0, len(regionPeers))
	for _, pair := range regionPeers {
		mailboxes = append(mailboxes, addrMailboxPair{
			addr:    pair.fsm.regionID(),
			mailbox: newMailbox(pair.sender, pair.fsm),
		})
	}
	bs.router.registerAll(mailboxes)
	// Make sure Msg::Start is the first message each FSM received.
	bs.router.sendControl(Msg{Type: MsgTypeStoreStart, Data: store})
	for _, mb := range mailboxes {
		err = bs.router.send(mb.addr, Msg{Type: MsgTypeStart})
		if err != nil {
			panic(err)
		}
	}

	bs.applySystem.start("apply", applyPollerBuilder)
	engines := builder.engines
	workers.splitCheckWorker.start(&splitCheckRunner{
		engine:          engines.kv,
		router:          bs.router,
		coprocessorHost: workers.coprocessorHost,
	})
	cfg := builder.cfg
	workers.regionWorker.start(newRegionRunner(engines, snapMgr, cfg.SnapApplyBatchSize, cfg.CleanStalePeerDelay))
	workers.raftLogGCWorker.start(&raftLogGCRunner{})
	workers.compactWorker.start(&compactRunner{engine: engines.kv})
	pdRunner := newPDRunner(store.Id, builder.pdClient, bs.router, engines.kv, workers.pdWorker.scheduler)
	workers.pdWorker.start(pdRunner)
	workers.computeHashWorker.start(&computeHashRunner{router: bs.router})
	bs.workers = workers
	return nil
}

func (bs *raftBatchSystem) scheduleApplySystem(regionPeers []senderPeerFsmPair) {
	mailboxes := make([]addrMailboxPair, 0, len(regionPeers))
	for _, pair := range regionPeers {
		peer := pair.fsm
		sender, applyFsm := newApplyFsmFromPeer(peer)
		mailboxes = append(mailboxes, addrMailboxPair{
			addr:    peer.regionID(),
			mailbox: newMailbox(sender, applyFsm),
		})
	}
	bs.router.registerAll(mailboxes)
}

func (bs *raftBatchSystem) shutDown() {
	if bs.workers == nil {
		return
	}
	workers := bs.workers
	bs.workers = nil
	stopTask := task{tp: taskTypeStop}
	workers.splitCheckWorker.scheduler <- stopTask
	workers.regionWorker.scheduler <- stopTask
	workers.raftLogGCWorker.scheduler <- stopTask
	workers.computeHashWorker.scheduler <- stopTask
	workers.pdWorker.scheduler <- stopTask
	workers.compactWorker.scheduler <- stopTask
	bs.applySystem.shutdown()
	bs.system.shutdown()
	bs.workers.wg.Wait()
	bs.workers.coprocessorHost.shutdown()
}

func createRaftBatchSystem(cfg *Config) (*router, *raftBatchSystem) {
	return nil, nil // TODO: stub
}

/// Checks if the message is targeting a stale peer.
///
/// Returns true means the message can be dropped silently.
func (d *storeFsmDelegate) checkMsg(msg *rspb.RaftMessage) (bool, error) {
	return false, nil // TODO: stub
}

func (d *storeFsmDelegate) onRaftMessage(msg *rspb.RaftMessage) error {
	return nil // TODO: stub
}

/// If target peer doesn't exist, create it.
///
/// return false to indicate that target peer is in invalid state or
/// doesn't exist and can't be created.
func (d *storeFsmDelegate) maybeCreatePeer(regionID uint64, msg *rspb.RaftMessage) (bool, error) {
	return false, nil // TODO: stub
}

func (d *storeFsmDelegate) onCompactionFinished(event *rocksdb.CompactedEvent) {
	// TODO: stub
}

func (d *storeFsmDelegate) onCompactCheckTick() {
	// TODO: stub
}

func (d *storeFsmDelegate) storeHeartbeatPD() {
	// TODO: stub
}

func (d *storeFsmDelegate) onPDStoreHearbeatTick() {
	// TODO: stub
}

func (d *storeFsmDelegate) handleSnapMgrGC() error {
	return nil // TODO: stub
}

func (d *storeFsmDelegate) onSnapMgrGC() {
	// TODO: stub
}

func (d *storeFsmDelegate) onValidateSSTResult(ssts []*import_sstpb.SSTMeta) {
	// TODO: importer
}

func (d *storeFsmDelegate) onCleanupImportSST() error {
	return nil // TODO: importer
}

func (d *storeFsmDelegate) onComputeHashTick() {
	// TODO: stub
}

func (d *storeFsmDelegate) onCleanUpImportSSTTick() {
	// TODO: importer
}

func (d *storeFsmDelegate) clearRegionSizeInRange(startKey, endKey []byte) {
	// TODO: stub
}

type regionIDDeclinedBytesPair struct {
	regionID      uint64
	declinedBytes uint64
}

func calcRegionDeclinedBytes(event *rocksdb.CompactedEvent,
	regionRanges *lockstore.MemStore, bytesThreshold uint64) []regionIDDeclinedBytesPair {
	return nil // TODO: stub
}

func isRangeCovered(regionRanges *lockstore.MemStore,
	getRegion func(id uint64) *metapb.Region, start, end []byte) bool {
	return false // TODO: stub
}
