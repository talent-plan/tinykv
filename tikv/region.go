package tikv

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
)

var (
	InternalKeyPrefix        = []byte(`i`)
	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
	InternalSafePointKey     = append(InternalKeyPrefix, "safepoint"...)
)

func InternalRegionMetaKey(regionId uint64) []byte {
	return []byte(string(InternalRegionMetaPrefix) + strconv.FormatUint(regionId, 10))
}

type regionCtx struct {
	meta            *metapb.Region
	regionEpoch     unsafe.Pointer // *metapb.RegionEpoch
	startKey        []byte
	endKey          []byte
	approximateSize int64
	diff            int64

	latches   map[uint64]*sync.WaitGroup
	latchesMu sync.RWMutex

	refCount sync.WaitGroup
	parent   unsafe.Pointer // Parent is used to wait for all latches being released.

	leaderChecker raftstore.LeaderChecker
}

func newRegionCtx(meta *metapb.Region, parent *regionCtx, checker raftstore.LeaderChecker) *regionCtx {
	regCtx := &regionCtx{
		meta:          meta,
		latches:       make(map[uint64]*sync.WaitGroup),
		regionEpoch:   unsafe.Pointer(meta.GetRegionEpoch()),
		parent:        unsafe.Pointer(parent),
		leaderChecker: checker,
	}
	regCtx.startKey = regCtx.rawStartKey()
	regCtx.endKey = regCtx.rawEndKey()
	regCtx.refCount.Add(1)
	return regCtx
}

func (ri *regionCtx) getRegionEpoch() *metapb.RegionEpoch {
	return (*metapb.RegionEpoch)(atomic.LoadPointer(&ri.regionEpoch))
}

func (ri *regionCtx) updateRegionEpoch(epoch *metapb.RegionEpoch) {
	atomic.StorePointer(&ri.regionEpoch, (unsafe.Pointer)(epoch))
}

func (ri *regionCtx) rawStartKey() []byte {
	if len(ri.meta.StartKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.StartKey, nil)
	if err != nil {
		panic("invalid region start key")
	}
	return rawKey
}

func (ri *regionCtx) rawEndKey() []byte {
	if len(ri.meta.EndKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.EndKey, nil)
	if err != nil {
		panic("invalid region end key")
	}
	return rawKey
}

func (ri *regionCtx) lessThanStartKey(key []byte) bool {
	return bytes.Compare(key, ri.startKey) < 0
}

func (ri *regionCtx) greaterEqualEndKey(key []byte) bool {
	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) >= 0
}

func (ri *regionCtx) greaterThanEndKey(key []byte) bool {
	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) > 0
}

func (ri *regionCtx) unmarshal(data []byte) error {
	ri.approximateSize = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	ri.meta = &metapb.Region{}
	err := ri.meta.Unmarshal(data)
	if err != nil {
		return errors.Trace(err)
	}
	ri.latches = make(map[uint64]*sync.WaitGroup)
	ri.startKey = ri.rawStartKey()
	ri.endKey = ri.rawEndKey()
	ri.regionEpoch = unsafe.Pointer(ri.meta.RegionEpoch)
	ri.refCount.Add(1)
	return nil
}

func (ri *regionCtx) marshal() []byte {
	data := make([]byte, 8+ri.meta.Size())
	binary.LittleEndian.PutUint64(data, uint64(ri.approximateSize))
	_, err := ri.meta.MarshalTo(data[8:])
	if err != nil {
		log.Error(err)
	}
	return data
}

func (ri *regionCtx) tryAcquireLatches(hashVals []uint64) (bool, *sync.WaitGroup) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	ri.latchesMu.Lock()
	defer ri.latchesMu.Unlock()
	for _, hashVal := range hashVals {
		if wg, ok := ri.latches[hashVal]; ok {
			return false, wg
		}
	}
	for _, hashVal := range hashVals {
		ri.latches[hashVal] = wg
	}
	return true, nil
}

func (ri *regionCtx) AcquireLatches(hashVals []uint64) {
	start := time.Now()
	for {
		ok, wg := ri.tryAcquireLatches(hashVals)
		if ok {
			dur := time.Since(start)
			metrics.LatchWait.Observe(dur.Seconds())
			if dur > time.Millisecond*50 {
				log.Warnf("region %d acquire %d locks takes %v", ri.meta.Id, len(hashVals), dur)
			}
			return
		}
		wg.Wait()
	}
}

func (ri *regionCtx) ReleaseLatches(hashVals []uint64) {
	ri.latchesMu.Lock()
	defer ri.latchesMu.Unlock()
	wg := ri.latches[hashVals[0]]
	for _, hashVal := range hashVals {
		delete(ri.latches, hashVal)
	}
	wg.Done()
}

func (ri *regionCtx) waitParent() {
	parent := (*regionCtx)(atomic.LoadPointer(&ri.parent))
	if parent != nil {
		// Wait for the parent region reference decrease to zero, so the latches would be clean.
		parent.refCount.Wait()
		// TODO: the txnKeysMap in parent is discarded, if a large transaction failed
		// and the client is down, leaves many locks, we can only resolve a single key at a time.
		// Need to find a way to address this later.
		atomic.StorePointer(&ri.parent, nil)
	}
}

type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

type RegionManager interface {
	GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error)
	Close() error
}

type regionManager struct {
	storeMeta *metapb.Store
	mu        sync.RWMutex
	regions   map[uint64]*regionCtx
}

func (rm *regionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != rm.storeMeta.Id {
		return nil, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	rm.mu.RLock()
	ri := rm.regions[ctx.RegionId]
	if ri != nil {
		ri.refCount.Add(1)
	}
	rm.mu.RUnlock()
	if ri == nil {
		return nil, &errorpb.Error{
			Message: "region not found",
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: ctx.GetRegionId(),
			},
		}
	}
	// Region epoch does not match.
	if rm.isEpochStale(ri.getRegionEpoch(), ctx.GetRegionEpoch()) {
		ri.refCount.Done()
		return nil, &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{{
					Id:          ri.meta.Id,
					StartKey:    ri.meta.StartKey,
					EndKey:      ri.meta.EndKey,
					RegionEpoch: ri.getRegionEpoch(),
					Peers:       ri.meta.Peers,
				}},
			},
		}
	}
	ri.waitParent()
	return ri, nil
}

func (rm *regionManager) isEpochStale(lhs, rhs *metapb.RegionEpoch) bool {
	return lhs.GetConfVer() != rhs.GetConfVer() || lhs.GetVersion() != rhs.GetVersion()
}

type RaftRegionManager struct {
	regionManager
	router *raftstore.RaftstoreRouter
}

func NewRaftRegionManager(store *metapb.Store, router *raftstore.RaftstoreRouter) *RaftRegionManager {
	return &RaftRegionManager{
		router: router,
		regionManager: regionManager{
			storeMeta: store,
			regions:   make(map[uint64]*regionCtx),
		},
	}
}

func (rm *RaftRegionManager) OnPeerCreate(ctx *raftstore.PeerEventContext, region *metapb.Region) {
	rm.mu.Lock()
	rm.regions[ctx.RegionId] = newRegionCtx(region, nil, ctx.LeaderChecker)
	rm.mu.Unlock()
}

func (rm *RaftRegionManager) OnPeerApplySnap(ctx *raftstore.PeerEventContext, region *metapb.Region) {
	rm.mu.Lock()
	rm.regions[ctx.RegionId] = newRegionCtx(region, nil, ctx.LeaderChecker)
	rm.mu.Unlock()
}

func (rm *RaftRegionManager) OnPeerDestroy(ctx *raftstore.PeerEventContext) {
	rm.mu.Lock()
	region := rm.regions[ctx.RegionId]
	region.waitParent()
	delete(rm.regions, ctx.RegionId)
	rm.mu.Unlock()
	region.refCount.Done()
}

func (rm *RaftRegionManager) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*raftstore.PeerEventContext) {
	rm.mu.Lock()
	oldRegion := rm.regions[derived.Id]
	oldRegion.waitParent()
	for i, region := range regions {
		rm.regions[region.Id] = newRegionCtx(region, oldRegion, peers[i].LeaderChecker)
	}
	rm.mu.Unlock()
	oldRegion.refCount.Done()
}

func (rm *RaftRegionManager) OnRegionConfChange(ctx *raftstore.PeerEventContext, epoch *metapb.RegionEpoch) {
	rm.mu.RLock()
	region := rm.regions[ctx.RegionId]
	rm.mu.RUnlock()
	region.updateRegionEpoch(epoch)
}

func (rm *RaftRegionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
	regionCtx, err := rm.regionManager.GetRegionFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if err := regionCtx.leaderChecker.IsLeader(ctx, rm.router); err != nil {
		regionCtx.refCount.Done()
		return nil, err
	}
	return regionCtx, nil
}

func (rm *RaftRegionManager) Close() error {
	return nil
}

type StandAloneRegionManager struct {
	regionManager
	db         *badger.DB
	pdc        pd.Client
	clusterID  uint64
	regionSize int64
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

func NewStandAloneRegionManager(db *badger.DB, opts RegionOptions, pdc pd.Client) *StandAloneRegionManager {
	var err error
	clusterID := pdc.GetClusterID(context.TODO())
	log.Infof("cluster id %v", clusterID)
	rm := &StandAloneRegionManager{
		db:         db,
		pdc:        pdc,
		clusterID:  clusterID,
		regionSize: opts.RegionSize,
		closeCh:    make(chan struct{}),
		regionManager: regionManager{
			regions:   make(map[uint64]*regionCtx),
			storeMeta: new(metapb.Store),
		},
	}
	err = rm.db.View(func(txn *badger.Txn) error {
		item, err1 := txn.Get(InternalStoreMetaKey)
		if err1 != nil {
			return err1
		}
		val, err1 := item.Value()
		if err1 != nil {
			return err1
		}
		err1 = rm.storeMeta.Unmarshal(val)
		if err1 != nil {
			return err1
		}
		// load region meta
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := InternalRegionMetaPrefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err1 = item.Value()
			if err1 != nil {
				return err1
			}
			r := new(regionCtx)
			err = r.unmarshal(val)
			if err != nil {
				return errors.Trace(err)
			}
			rm.regions[r.meta.Id] = r
		}
		return nil
	})
	if err != nil && err != badger.ErrKeyNotFound {
		log.Fatal(err)
	}
	if rm.storeMeta.Id == 0 {
		err = rm.initStore(opts.StoreAddr)
		if err != nil {
			log.Fatal(err)
		}
	}
	rm.storeMeta.Address = opts.StoreAddr
	rm.pdc.PutStore(context.TODO(), rm.storeMeta)
	rm.wg.Add(2)
	go rm.runSplitWorker()
	go rm.storeHeartBeatLoop()
	return rm
}

func (rm *StandAloneRegionManager) initStore(storeAddr string) error {
	log.Info("initializing store")
	ids, err := rm.allocIDs(3)
	if err != nil {
		return err
	}
	storeID, regionID, peerID := ids[0], ids[1], ids[2]
	rm.storeMeta.Id = storeID
	rm.storeMeta.Address = storeAddr
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	rootRegion := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{&metapb.Peer{Id: peerID, StoreId: storeID}},
	}
	rm.regions[rootRegion.Id] = newRegionCtx(rootRegion, nil, nil)
	_, err = rm.pdc.Bootstrap(ctx, rm.storeMeta, rootRegion)
	cancel()
	if err != nil {
		log.Fatal("Initialize failed: ", err)
	}
	rm.initialSplit(rootRegion)
	storeBuf, err := rm.storeMeta.Marshal()
	if err != nil {
		log.Fatal("%+v", err)
	}
	err = rm.db.Update(func(txn *badger.Txn) error {
		txn.Set(InternalStoreMetaKey, storeBuf)
		for rid, region := range rm.regions {
			regionBuf := region.marshal()
			err = txn.Set(InternalRegionMetaKey(rid), regionBuf)
			if err != nil {
				log.Fatal("%+v", err)
			}
		}
		return nil
	})
	for _, region := range rm.regions {
		req := &pdpb.RegionHeartbeatRequest{
			Region:          region.meta,
			Leader:          region.meta.Peers[0],
			ApproximateSize: uint64(region.approximateSize),
		}
		rm.pdc.ReportRegion(req)
	}
	log.Info("Initialize success")
	return nil
}

// initSplit splits the cluster into multiple regions.
func (rm *StandAloneRegionManager) initialSplit(root *metapb.Region) {
	root.EndKey = codec.EncodeBytes(nil, []byte{'m'})
	root.RegionEpoch.Version = 2
	rm.regions[root.Id] = newRegionCtx(root, nil, nil)
	preSplitStartKeys := [][]byte{{'m'}, {'n'}, {'t'}, {'u'}}
	ids, err := rm.allocIDs(len(preSplitStartKeys) * 2)
	if err != nil {
		log.Fatal(err)
	}
	for i, startKey := range preSplitStartKeys {
		var endKey []byte
		if i < len(preSplitStartKeys)-1 {
			endKey = codec.EncodeBytes(nil, preSplitStartKeys[i+1])
		}
		newRegion := &metapb.Region{
			Id:          ids[i*2],
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{&metapb.Peer{Id: ids[i*2+1], StoreId: rm.storeMeta.Id}},
			StartKey:    codec.EncodeBytes(nil, startKey),
			EndKey:      endKey,
		}
		rm.regions[newRegion.Id] = newRegionCtx(newRegion, nil, nil)
	}
}

func (rm *StandAloneRegionManager) allocIDs(n int) ([]uint64, error) {
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id, err := rm.pdc.AllocID(context.Background())
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids[i] = id
	}
	return ids, nil
}

func (rm *StandAloneRegionManager) storeHeartBeatLoop() {
	defer rm.wg.Done()
	ticker := time.Tick(time.Second * 3)
	for {
		select {
		case <-rm.closeCh:
			return
		case <-ticker:
		}
		storeStats := new(pdpb.StoreStats)
		storeStats.StoreId = rm.storeMeta.Id
		storeStats.Available = 1024 * 1024 * 1024
		rm.mu.RLock()
		storeStats.RegionCount = uint32(len(rm.regions))
		rm.mu.RUnlock()
		storeStats.Capacity = 2048 * 1024 * 1024
		rm.pdc.StoreHeartbeat(context.Background(), storeStats)
	}
}

type keySample struct {
	key      []byte
	leftSize int64
}

// sampler samples keys in a region for later pick a split key.
type sampler struct {
	samples   [64]keySample
	length    int
	step      int
	scanned   int
	totalSize int64
}

func newSampler() *sampler {
	return &sampler{step: 1}
}

func (s *sampler) shrinkIfNeeded() {
	if s.length < len(s.samples) {
		return
	}
	for i := 0; i < len(s.samples)/2; i++ {
		s.samples[i], s.samples[i*2] = s.samples[i*2], s.samples[i]
	}
	s.length /= 2
	s.step *= 2
}

func (s *sampler) shouldSample() bool {
	// It's an optimization for 's.scanned % s.step == 0'
	return s.scanned&(s.step-1) == 0
}

func (s *sampler) scanKey(key []byte, size int64) {
	s.totalSize += size
	s.scanned++
	if s.shouldSample() {
		sample := s.samples[s.length]
		// safe copy the key.
		sample.key = append(sample.key[:0], key...)
		sample.leftSize = s.totalSize
		s.samples[s.length] = sample
		s.length++
		s.shrinkIfNeeded()
	}
}

func (s *sampler) getSplitKeyAndSize() ([]byte, int64) {
	targetSize := s.totalSize * 2 / 3
	for _, sample := range s.samples[:s.length] {
		if sample.leftSize >= targetSize {
			return sample.key, sample.leftSize
		}
	}
	return []byte{}, 0
}

func (rm *StandAloneRegionManager) runSplitWorker() {
	defer rm.wg.Done()
	ticker := time.NewTicker(time.Second * 5)
	var regionsToCheck []*regionCtx
	var regionsToSave []*regionCtx
	for {
		regionsToCheck = regionsToCheck[:0]
		rm.mu.RLock()
		for _, ri := range rm.regions {
			if ri.approximateSize+atomic.LoadInt64(&ri.diff) > rm.regionSize*3/2 {
				regionsToCheck = append(regionsToCheck, ri)
			}
		}
		rm.mu.RUnlock()
		for _, ri := range regionsToCheck {
			rm.splitCheckRegion(ri)
		}

		regionsToSave = regionsToSave[:0]
		rm.mu.RLock()
		for _, ri := range rm.regions {
			if atomic.LoadInt64(&ri.diff) > rm.regionSize/8 {
				regionsToSave = append(regionsToSave, ri)
			}
		}
		rm.mu.RUnlock()
		rm.saveSize(regionsToSave)
		select {
		case <-rm.closeCh:
			return
		case <-ticker.C:
		}
	}
}

func (rm *StandAloneRegionManager) saveSize(regionsToSave []*regionCtx) {
	err1 := rm.db.Update(func(txn *badger.Txn) error {
		for _, ri := range regionsToSave {
			ri.approximateSize += atomic.LoadInt64(&ri.diff)
			err := txn.Set(InternalRegionMetaKey(ri.meta.Id), ri.marshal())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error(err1)
	}
}

func (rm *StandAloneRegionManager) splitCheckRegion(region *regionCtx) error {
	s := newSampler()
	err := rm.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()
		for iter.Seek(region.startKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if region.greaterEqualEndKey(item.Key()) {
				break
			}
			s.scanKey(item.Key(), item.EstimatedSize())
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	// Need to update the diff to avoid split check again.
	atomic.StoreInt64(&region.diff, s.totalSize-region.approximateSize)
	if s.totalSize < rm.regionSize {
		return nil
	}
	splitKey, leftSize := s.getSplitKeyAndSize()
	log.Infof("region:%d leftSize %d, rightSize %d", region.meta.Id, leftSize, s.totalSize-leftSize)
	log.Info("splitKey", splitKey, err)
	err = rm.splitRegion(region, splitKey, s.totalSize, leftSize)
	if err != nil {
		log.Error(err)
	}
	return errors.Trace(err)
}

func (rm *StandAloneRegionManager) splitRegion(oldRegionCtx *regionCtx, splitKey []byte, oldSize, leftSize int64) error {
	oldRegionCtx.waitParent()
	oldRegion := oldRegionCtx.meta
	rightMeta := &metapb.Region{
		Id:       oldRegion.Id,
		StartKey: codec.EncodeBytes(nil, splitKey),
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version + 1,
		},
		Peers: oldRegion.Peers,
	}
	right := newRegionCtx(rightMeta, oldRegionCtx, nil)
	right.approximateSize = oldSize - leftSize
	id, err := rm.pdc.AllocID(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	leftMeta := &metapb.Region{
		Id:       id,
		StartKey: oldRegion.StartKey,
		EndKey:   codec.EncodeBytes(nil, splitKey),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: oldRegion.Peers,
	}
	left := newRegionCtx(leftMeta, oldRegionCtx, nil)
	left.approximateSize = leftSize
	err1 := rm.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(InternalRegionMetaKey(left.meta.Id), left.marshal())
		if err != nil {
			return errors.Trace(err)
		}
		err = txn.Set(InternalRegionMetaKey(right.meta.Id), right.marshal())
		return errors.Trace(err)
	})
	if err1 != nil {
		return errors.Trace(err1)
	}
	rm.mu.Lock()
	rm.regions[left.meta.Id] = left
	rm.regions[right.meta.Id] = right
	rm.mu.Unlock()
	oldRegionCtx.refCount.Done()
	rm.pdc.ReportRegion(&pdpb.RegionHeartbeatRequest{
		Region:          right.meta,
		Leader:          right.meta.Peers[0],
		ApproximateSize: uint64(right.approximateSize),
	})
	rm.pdc.ReportRegion(&pdpb.RegionHeartbeatRequest{
		Region:          left.meta,
		Leader:          left.meta.Peers[0],
		ApproximateSize: uint64(left.approximateSize),
	})
	log.Infof("region %d split to left %d with size %d and right %d with size %d",
		oldRegion.Id, left.meta.Id, left.approximateSize, right.meta.Id, right.approximateSize)
	return nil
}

func (rm *StandAloneRegionManager) Close() error {
	close(rm.closeCh)
	rm.wg.Wait()
	return nil
}
