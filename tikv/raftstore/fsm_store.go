package raftstore

import (
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
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
	pdScheduler          chan<- task
	raftLogGCScheduler   chan<- task
	store                *metapb.Store
	regionScheduler      chan<- task
	splitCheckScheduler  chan<- task
	computeHashScheduler chan<- task
	cleanUpSSTScheduler  chan<- task
	ticker               *ticker
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
}

func newStoreFsm(cfg *Config) (chan<- Msg, *storeFsm) {
	ch := make(chan Msg, cfg.NotifyCapacity)
	fsm := &storeFsm{
		consistencyCheckTime: map[uint64]time.Time{},
		receiver:             (<-chan Msg)(ch),
	}
	return (chan<- Msg)(ch), fsm
}

func (s *storeFsm) isStopped() bool {
	return s.stopped
}

type storeFsmDelegate struct {
	*storeFsm
	ctx *PollContext
}

func (d *storeFsmDelegate) onTick(tick StoreTick) {
	// TODO: stub
	switch tick {
	case StoreTickCompactCheck:
	case StoreTickPdStoreHeartbeat:
	case StoreTickSnapGC:
	case StoreTickCompactLockCF:
	case StoreTickConsistencyCheck:
	case StoreTickCleanupImportSSI:
	}
}

func (d *storeFsmDelegate) handleMessages(msgs []Msg) {
	// TODO: stub
	for _, msg := range msgs {
		switch msg.Type {
		case MsgTypeStoreRaftMessage:
		case MsgTypeStoreSnapshotStats:
		case MsgTypeStoreValidateSSTResult:
		case MsgTypeStoreClearRegionSizeInRange:
		case MsgTypeStoreCompactedEvent:
		case MsgTypeStoreTick:
		case MsgTypeStoreStart:
		}
	}
}

func (d *storeFsmDelegate) start(store *metapb.Store) {
	// TODO: stub
}

type raftPoller struct {
	tag              string
	storeMsgBuf      []Msg
	peerMsgBuf       []Msg
	pollCtx          *PollContext
	pendingProposals []*RegionProposal
	messagesPerTick  int
}

func (p *raftPoller) handleRaftReady() {
	return // TODO: stub
}

func (p *raftPoller) begin(batchSize int) {
	return // TODO: stub
}

func (p *raftPoller) handleControl(storeFsm fsm) (pause bool, chLen int) {
	return // TODO: stub
}

func (p *raftPoller) handleNormal(normal fsm) (pause bool, chLen int) {
	return // TODO: stub
}

func (p *raftPoller) end(normals []fsm) {
	return // TODO: stub
}

type raftPollerBuilder struct {
	cfg                  *Config
	store                *metapb.Store
	pdScheduler          chan<- task
	raftLogGCScheduler   chan<- task
	computeHashScheduler chan<- task
	splitCheckScheduler  chan<- task
	cleanUpSSTScheduler  chan<- task
	regionScheduler      chan<- task
	applyRouter          *router
	router               *router
	compactScheduler     chan<- task
	storeMetaMu          sync.Mutex
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
	return nil, nil // TODO: stub
}

func (b *raftPollerBuilder) clearStaleMeta(kvWB, raftWB *WriteBatch, originState *rspb.RegionLocalState) {
	// TODO: stub
}

/// clearStaleData clean up all possible garbage data.
func (b *raftPollerBuilder) clearStaleData(meta *storeMeta) error {
	return nil // TODO: stub
}

func (b *raftPollerBuilder) build() *raftPoller {
	return nil // TODO: stub
}

type workers struct {
	pdWorker          *worker
	raftLogGCWorker   *worker
	computeHashWorker *worker
	splitCheckWorker  *worker
	cleanUpSSTWorker  *worker
	regionWorker      *worker
	compactWorker     *worker
	coprocessorHost   *CoprocessorHost
}

type raftBatchSystem struct {
	system      *batchSystem
	applyRouter *router
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
	coprocessorHost *CoprocessorHost) error {
	return nil // TODO: stub
}

func (bs *raftBatchSystem) startSystem(
	workers *workers, regionPeers []senderPeerFsmPair, builder *raftPollerBuilder) error {
	return nil // TODO: stub
}

func (bs *raftBatchSystem) shutDown() {
	// TODO: stub
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
	return nil// TODO: stub
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
