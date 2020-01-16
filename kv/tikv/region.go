package tikv

// const (
// 	Follower = iota
// 	Leader
// )

// var (
// 	InternalKeyPrefix        = []byte(`i`)
// 	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
// 	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
// 	InternalSafePointKey     = append(InternalKeyPrefix, "safepoint"...)
// )

// func InternalRegionMetaKey(regionId uint64) []byte {
// 	return []byte(string(InternalRegionMetaPrefix) + strconv.FormatUint(regionId, 10))
// }

// type regionCtx struct {
// 	meta            *metapb.Region
// 	regionEpoch     unsafe.Pointer // *metapb.RegionEpoch
// 	startKey        []byte
// 	endKey          []byte
// 	approximateSize int64
// 	diff            int64

// 	latches   map[uint64]*sync.WaitGroup
// 	latchesMu sync.RWMutex

// 	refCount sync.WaitGroup
// 	parent   unsafe.Pointer // Parent is used to wait for all latches being released.

// 	leaderChecker raftstore.LeaderChecker
// }

// func newRegionCtx(meta *metapb.Region, parent *regionCtx, checker raftstore.LeaderChecker) *regionCtx {
// 	regCtx := &regionCtx{
// 		meta:          meta,
// 		latches:       make(map[uint64]*sync.WaitGroup),
// 		regionEpoch:   unsafe.Pointer(meta.GetRegionEpoch()),
// 		parent:        unsafe.Pointer(parent),
// 		leaderChecker: checker,
// 	}
// 	regCtx.startKey = regCtx.rawStartKey()
// 	regCtx.endKey = regCtx.rawEndKey()
// 	regCtx.refCount.Add(1)
// 	return regCtx
// }

// func (ri *regionCtx) updateRegionMeta(meta *metapb.Region) {
// 	ri.meta = meta
// 	ri.startKey = ri.rawStartKey()
// 	ri.endKey = ri.rawEndKey()
// 	ri.updateRegionEpoch(meta.GetRegionEpoch())
// }

// func (ri *regionCtx) getRegionEpoch() *metapb.RegionEpoch {
// 	return (*metapb.RegionEpoch)(atomic.LoadPointer(&ri.regionEpoch))
// }

// func (ri *regionCtx) updateRegionEpoch(epoch *metapb.RegionEpoch) {
// 	atomic.StorePointer(&ri.regionEpoch, (unsafe.Pointer)(epoch))
// }

// func (ri *regionCtx) rawStartKey() []byte {
// 	if len(ri.meta.StartKey) == 0 {
// 		return nil
// 	}
// 	_, rawKey, err := codec.DecodeBytes(ri.meta.StartKey, nil)
// 	if err != nil {
// 		panic("invalid region start key")
// 	}
// 	return rawKey
// }

// func (ri *regionCtx) rawEndKey() []byte {
// 	if len(ri.meta.EndKey) == 0 {
// 		return nil
// 	}
// 	_, rawKey, err := codec.DecodeBytes(ri.meta.EndKey, nil)
// 	if err != nil {
// 		panic("invalid region end key")
// 	}
// 	return rawKey
// }

// func (ri *regionCtx) lessThanStartKey(key []byte) bool {
// 	return bytes.Compare(key, ri.startKey) < 0
// }

// func (ri *regionCtx) greaterEqualEndKey(key []byte) bool {
// 	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) >= 0
// }

// func (ri *regionCtx) greaterThanEndKey(key []byte) bool {
// 	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) > 0
// }

// func (ri *regionCtx) unmarshal(data []byte) error {
// 	ri.approximateSize = int64(binary.LittleEndian.Uint64(data))
// 	data = data[8:]
// 	ri.meta = &metapb.Region{}
// 	err := ri.meta.Unmarshal(data)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}
// 	ri.latches = make(map[uint64]*sync.WaitGroup)
// 	ri.startKey = ri.rawStartKey()
// 	ri.endKey = ri.rawEndKey()
// 	ri.regionEpoch = unsafe.Pointer(ri.meta.RegionEpoch)
// 	ri.refCount.Add(1)
// 	return nil
// }

// func (ri *regionCtx) marshal() []byte {
// 	data := make([]byte, 8+ri.meta.Size())
// 	binary.LittleEndian.PutUint64(data, uint64(ri.approximateSize))
// 	_, err := ri.meta.MarshalTo(data[8:])
// 	if err != nil {
// 		log.Error(err)
// 	}
// 	return data
// }

// func (ri *regionCtx) tryAcquireLatches(hashVals []uint64) (bool, *sync.WaitGroup) {
// 	wg := new(sync.WaitGroup)
// 	wg.Add(1)
// 	ri.latchesMu.Lock()
// 	defer ri.latchesMu.Unlock()
// 	for _, hashVal := range hashVals {
// 		if wg, ok := ri.latches[hashVal]; ok {
// 			return false, wg
// 		}
// 	}
// 	for _, hashVal := range hashVals {
// 		ri.latches[hashVal] = wg
// 	}
// 	return true, nil
// }

// func (ri *regionCtx) AcquireLatches(hashVals []uint64) {
// 	start := time.Now()
// 	for {
// 		ok, wg := ri.tryAcquireLatches(hashVals)
// 		if ok {
// 			dur := time.Since(start)
// 			if dur > time.Millisecond*50 {
// 				log.Warnf("region %d acquire %d locks takes %v", ri.meta.Id, len(hashVals), dur)
// 			}
// 			return
// 		}
// 		wg.Wait()
// 	}
// }

// func (ri *regionCtx) ReleaseLatches(hashVals []uint64) {
// 	ri.latchesMu.Lock()
// 	defer ri.latchesMu.Unlock()
// 	wg := ri.latches[hashVals[0]]
// 	for _, hashVal := range hashVals {
// 		delete(ri.latches, hashVal)
// 	}
// 	wg.Done()
// }

// func (ri *regionCtx) waitParent() {
// 	parent := (*regionCtx)(atomic.LoadPointer(&ri.parent))
// 	if parent != nil {
// 		// Wait for the parent region reference decrease to zero, so the latches would be clean.
// 		parent.refCount.Wait()
// 		// TODO: the txnKeysMap in parent is discarded, if a large transaction failed
// 		// and the client is down, leaves many locks, we can only resolve a single key at a time.
// 		// Need to find a way to address this later.
// 		atomic.StorePointer(&ri.parent, nil)
// 	}
// }

// type RegionOptions struct {
// 	StoreAddr  string
// 	PDAddr     string
// 	RegionSize int64
// }

// type RegionManager interface {
// 	GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error)
// 	Close() error
// }

// type regionManager struct {
// 	storeMeta *metapb.Store
// 	mu        sync.RWMutex
// 	regions   map[uint64]*regionCtx
// }

// func (rm *regionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
// 	ctxPeer := ctx.GetPeer()
// 	if ctxPeer != nil && ctxPeer.GetStoreId() != rm.storeMeta.Id {
// 		return nil, &errorpb.Error{
// 			Message:       "store not match",
// 			StoreNotMatch: &errorpb.StoreNotMatch{},
// 		}
// 	}
// 	rm.mu.RLock()
// 	ri := rm.regions[ctx.RegionId]
// 	if ri != nil {
// 		ri.refCount.Add(1)
// 	}
// 	rm.mu.RUnlock()
// 	if ri == nil {
// 		return nil, &errorpb.Error{
// 			Message: "region not found",
// 			RegionNotFound: &errorpb.RegionNotFound{
// 				RegionId: ctx.GetRegionId(),
// 			},
// 		}
// 	}
// 	// Region epoch does not match.
// 	if rm.isEpochStale(ri.getRegionEpoch(), ctx.GetRegionEpoch()) {
// 		ri.refCount.Done()
// 		return nil, &errorpb.Error{
// 			Message: "stale epoch",
// 			EpochNotMatch: &errorpb.EpochNotMatch{
// 				CurrentRegions: []*metapb.Region{{
// 					Id:          ri.meta.Id,
// 					StartKey:    ri.meta.StartKey,
// 					EndKey:      ri.meta.EndKey,
// 					RegionEpoch: ri.getRegionEpoch(),
// 					Peers:       ri.meta.Peers,
// 				}},
// 			},
// 		}
// 	}
// 	ri.waitParent()
// 	return ri, nil
// }

// func (rm *regionManager) isEpochStale(lhs, rhs *metapb.RegionEpoch) bool {
// 	return lhs.GetConfVer() != rhs.GetConfVer() || lhs.GetVersion() != rhs.GetVersion()
// }

// type RaftRegionManager struct {
// 	regionManager
// 	router  *raftstore.RaftstoreRouter
// 	eventCh chan interface{}
// }

// func NewRaftRegionManager(store *metapb.Store, router *raftstore.RaftstoreRouter) *RaftRegionManager {
// 	m := &RaftRegionManager{
// 		router: router,
// 		regionManager: regionManager{
// 			storeMeta: store,
// 			regions:   make(map[uint64]*regionCtx),
// 		},
// 		eventCh: make(chan interface{}, 1024),
// 	}
// 	go m.runEventHandler()
// 	return m
// }

// type peerCreateEvent struct {
// 	ctx    *raftstore.PeerEventContext
// 	region *metapb.Region
// }

// func (rm *RaftRegionManager) OnPeerCreate(ctx *raftstore.PeerEventContext, region *metapb.Region) {
// 	rm.eventCh <- &peerCreateEvent{
// 		ctx:    ctx,
// 		region: region,
// 	}
// }

// type peerApplySnapEvent struct {
// 	ctx    *raftstore.PeerEventContext
// 	region *metapb.Region
// }

// func (rm *RaftRegionManager) OnPeerApplySnap(ctx *raftstore.PeerEventContext, region *metapb.Region) {
// 	rm.eventCh <- &peerApplySnapEvent{
// 		ctx:    ctx,
// 		region: region,
// 	}
// }

// type peerDestroyEvent struct {
// 	regionID uint64
// }

// func (rm *RaftRegionManager) OnPeerDestroy(ctx *raftstore.PeerEventContext) {
// 	rm.eventCh <- &peerDestroyEvent{regionID: ctx.RegionId}
// }

// type splitRegionEvent struct {
// 	derived *metapb.Region
// 	regions []*metapb.Region
// 	peers   []*raftstore.PeerEventContext
// }

// func (rm *RaftRegionManager) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*raftstore.PeerEventContext) {
// 	rm.eventCh <- &splitRegionEvent{
// 		derived: derived,
// 		regions: regions,
// 		peers:   peers,
// 	}
// }

// type regionConfChangeEvent struct {
// 	ctx   *raftstore.PeerEventContext
// 	epoch *metapb.RegionEpoch
// }

// func (rm *RaftRegionManager) OnRegionConfChange(ctx *raftstore.PeerEventContext, epoch *metapb.RegionEpoch) {
// 	rm.eventCh <- &regionConfChangeEvent{
// 		ctx:   ctx,
// 		epoch: epoch,
// 	}
// }

// type regionRoleChangeEvent struct {
// 	regionId uint64
// 	newState raft.StateType
// }

// func (rm *RaftRegionManager) OnRoleChange(regionId uint64, newState raft.StateType) {
// 	rm.eventCh <- &regionRoleChangeEvent{regionId: regionId, newState: newState}
// }

// func (rm *RaftRegionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
// 	regionCtx, err := rm.regionManager.GetRegionFromCtx(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if err := regionCtx.leaderChecker.IsLeader(ctx, rm.router); err != nil {
// 		regionCtx.refCount.Done()
// 		return nil, err
// 	}
// 	return regionCtx, nil
// }

// func (rm *RaftRegionManager) Close() error {
// 	return nil
// }

// func (rm *RaftRegionManager) runEventHandler() {
// 	for event := range rm.eventCh {
// 		switch x := event.(type) {
// 		case *peerCreateEvent:
// 			regCtx := newRegionCtx(x.region, nil, x.ctx.LeaderChecker)
// 			rm.mu.Lock()
// 			rm.regions[x.ctx.RegionId] = regCtx
// 			rm.mu.Unlock()
// 		case *splitRegionEvent:
// 			rm.mu.RLock()
// 			oldRegion := rm.regions[x.derived.Id]
// 			rm.mu.RUnlock()
// 			oldRegion.waitParent()
// 			rm.mu.Lock()
// 			for i, region := range x.regions {
// 				rm.regions[region.Id] = newRegionCtx(region, oldRegion, x.peers[i].LeaderChecker)
// 			}
// 			rm.mu.Unlock()
// 			oldRegion.refCount.Done()
// 		case *regionConfChangeEvent:
// 			rm.mu.RLock()
// 			region := rm.regions[x.ctx.RegionId]
// 			rm.mu.RUnlock()
// 			region.updateRegionEpoch(x.epoch)
// 		case *peerDestroyEvent:
// 			rm.mu.Lock()
// 			region := rm.regions[x.regionID]
// 			delete(rm.regions, x.regionID)
// 			rm.mu.Unlock()
// 			// We don't need to wait for parent for a destroyed peer.
// 			region.refCount.Done()
// 		case *peerApplySnapEvent:
// 			rm.mu.RLock()
// 			oldRegion := rm.regions[x.region.Id]
// 			rm.mu.RUnlock()
// 			oldRegion.waitParent()
// 			rm.mu.Lock()
// 			rm.regions[x.region.Id] = newRegionCtx(x.region, oldRegion, x.ctx.LeaderChecker)
// 			rm.mu.Unlock()
// 			oldRegion.refCount.Done()
// 		case *regionRoleChangeEvent:
// 			rm.mu.RLock()
// 			region := rm.regions[x.regionId]
// 			rm.mu.RUnlock()
// 			if bytes.Compare(region.startKey, []byte{}) == 0 {
// 				newRole := Follower
// 				if x.newState == raft.StateLeader {
// 					newRole = Leader
// 				}
// 				log.Infof("first region role change to newRole=%v", newRole)
// 			}
// 		}
// 	}
// }
