package tikv

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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
	meta     *metapb.Region
	startKey []byte
	endKey   []byte
	sizeHint int64
	diff     int64

	latches   map[uint64]*sync.WaitGroup
	latchesMu sync.RWMutex

	refCount sync.WaitGroup
	parent   *regionCtx // Parent is used to wait for all latches being released.
}

func newRegionCtx(meta *metapb.Region, parent *regionCtx) *regionCtx {
	regCtx := &regionCtx{
		meta:    meta,
		latches: make(map[uint64]*sync.WaitGroup),
		parent:  parent,
	}
	regCtx.startKey = regCtx.rawStartKey()
	regCtx.endKey = regCtx.rawEndKey()
	regCtx.refCount.Add(1)
	return regCtx
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
	ri.sizeHint = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	ri.meta = &metapb.Region{}
	err := ri.meta.Unmarshal(data)
	if err != nil {
		return errors.Trace(err)
	}
	ri.latches = make(map[uint64]*sync.WaitGroup)
	ri.startKey = ri.rawStartKey()
	ri.endKey = ri.rawEndKey()
	ri.refCount.Add(1)
	return nil
}

func (ri *regionCtx) marshal() []byte {
	data := make([]byte, 8+ri.meta.Size())
	binary.LittleEndian.PutUint64(data, uint64(ri.sizeHint))
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

func (ri *regionCtx) acquireLatches(hashVals []uint64) {
	start := time.Now()
	for {
		ok, wg := ri.tryAcquireLatches(hashVals)
		if ok {
			dur := time.Since(start)
			if dur > time.Millisecond*50 {
				log.Warnf("acquire %d locks takes %v", len(hashVals), dur)
			}
			return
		}
		wg.Wait()
	}
}

func (ri *regionCtx) releaseLatches(hashVals []uint64) {
	ri.latchesMu.Lock()
	defer ri.latchesMu.Unlock()
	wg := ri.latches[hashVals[0]]
	for _, hashVal := range hashVals {
		delete(ri.latches, hashVal)
	}
	wg.Done()
}

var (
	NumIndexDBs = 2
	NumRowDBs   = 2
)

func (ri *regionCtx) getDBIdx() int {
	if !isShardingEnabled {
		return 0
	}
	startKey := ri.startKey
	if len(startKey) > 2 && startKey[0] == 't' {
		shardByte := startKey[2]
		if startKey[1] == 'i' {
			return int(bits.Reverse8(shardByte)) % NumIndexDBs
		} else {
			return 4 + int(bits.Reverse8(shardByte))%NumRowDBs
		}
	}
	return 0
}

func (ri *regionCtx) waitParent() {
	ptr := unsafe.Pointer(ri.parent)
	parent := (*regionCtx)(atomic.LoadPointer(&ptr))
	if parent != nil {
		// Wait for the parent region reference decrease to zero, so the latches would be clean.
		parent.refCount.Wait()
		// TODO: the txnKeysMap in parent is discarded, if a large transaction failed
		// and the client is down, leaves many locks, we can only resolve a single key at a time.
		// Need to find a way to address this later.
		atomic.StorePointer(&ptr, nil)
	}
}

type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

type RegionManager struct {
	storeMeta  metapb.Store
	mu         sync.RWMutex
	regions    map[uint64]*regionCtx
	dbs        []*badger.DB
	pdc        Client
	clusterID  uint64
	regionSize int64
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

func NewRegionManager(dbs []*badger.DB, opts RegionOptions) *RegionManager {
	pdc, err := NewClient(opts.PDAddr, "")
	if err != nil {
		log.Fatal(err)
	}
	clusterID := pdc.GetClusterID(context.TODO())
	log.Infof("cluster id %v", clusterID)
	rm := &RegionManager{
		dbs:        dbs,
		pdc:        pdc,
		clusterID:  clusterID,
		regions:    make(map[uint64]*regionCtx),
		regionSize: opts.RegionSize,
		closeCh:    make(chan struct{}),
	}
	err = rm.dbs[0].View(func(txn *badger.Txn) error {
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
		it := txn.NewIterator(badger.DefaultIteratorOptions)
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
	rm.pdc.PutStore(context.TODO(), &rm.storeMeta)
	rm.wg.Add(2)
	go rm.runSplitWorker()
	go rm.storeHeartBeatLoop()
	return rm
}

func (rm *RegionManager) initStore(storeAddr string) error {
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
	rm.regions[rootRegion.Id] = newRegionCtx(rootRegion, nil)
	err = rm.pdc.Bootstrap(ctx, &rm.storeMeta, rootRegion)
	cancel()
	if err != nil {
		log.Fatal("Initialize failed: ", err)
	}
	rm.initialSplit(rootRegion)
	storeBuf, err := rm.storeMeta.Marshal()
	if err != nil {
		log.Fatal("%+v", err)
	}
	err = rm.dbs[0].Update(func(txn *badger.Txn) error {
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
		rm.pdc.ReportRegion(region)
	}
	log.Info("Initialize success")
	return nil
}

// initSplit splits the cluster into multiple regions.
func (rm *RegionManager) initialSplit(root *metapb.Region) {
	root.EndKey = codec.EncodeBytes(nil, []byte{'m'})
	root.RegionEpoch.Version = 2
	rm.regions[root.Id] = newRegionCtx(root, nil)
	var preSplitStartKeys [][]byte
	if IsShardingEnabled() {
		preSplitStartKeys = [][]byte{
			{'m'},
			{'n'},
			{'t', 'i', 0},
			{'t', 'i', 64},
			{'t', 'i', 128},
			{'t', 'i', 192},
			{'t', 'r', 0},
			{'t', 'r', 64},
			{'t', 'r', 128},
			{'t', 'r', 192},
			{'u'},
		}
	} else {
		preSplitStartKeys = [][]byte{{'m'}, {'n'}, {'t'}, {'u'}}
	}
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
		rm.regions[newRegion.Id] = newRegionCtx(newRegion, nil)
	}
}

func (rm *RegionManager) allocIDs(n int) ([]uint64, error) {
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

func (rm *RegionManager) storeHeartBeatLoop() {
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

func (rm *RegionManager) getRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
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
	if rm.isEpochStale(ri.meta.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		ri.refCount.Done()
		return nil, &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{ri.meta},
			},
		}
	}
	ri.waitParent()
	return ri, nil
}

func (rm *RegionManager) isEpochStale(lhs, rhs *metapb.RegionEpoch) bool {
	return lhs.GetConfVer() != rhs.GetConfVer() || lhs.GetVersion() != rhs.GetVersion()
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

func (rm *RegionManager) runSplitWorker() {
	defer rm.wg.Done()
	ticker := time.NewTicker(time.Second * 5)
	var regionsToCheck []*regionCtx
	var regionsToSave []*regionCtx
	for {
		regionsToCheck = regionsToCheck[:0]
		rm.mu.RLock()
		for _, ri := range rm.regions {
			if ri.sizeHint+atomic.LoadInt64(&ri.diff) > rm.regionSize*3/2 {
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
		rm.saveSizeHint(regionsToSave)
		select {
		case <-rm.closeCh:
			return
		case <-ticker.C:
		}
	}
}

func (rm *RegionManager) saveSizeHint(regionsToSave []*regionCtx) {
	err1 := rm.dbs[0].Update(func(txn *badger.Txn) error {
		for _, ri := range regionsToSave {
			ri.sizeHint += atomic.LoadInt64(&ri.diff)
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

func (rm *RegionManager) splitCheckRegion(region *regionCtx) error {
	s := newSampler()
	err := rm.dbs[region.getDBIdx()].View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
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
	atomic.StoreInt64(&region.diff, s.totalSize-region.sizeHint)
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

func (rm *RegionManager) splitRegion(oldRegionCtx *regionCtx, splitKey []byte, oldSize, leftSize int64) error {
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
	right := newRegionCtx(rightMeta, oldRegionCtx)
	right.sizeHint = oldSize - leftSize
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
	left := newRegionCtx(leftMeta, oldRegionCtx)
	left.sizeHint = leftSize
	err1 := rm.dbs[0].Update(func(txn *badger.Txn) error {
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
	rm.pdc.ReportRegion(right)
	rm.pdc.ReportRegion(left)
	log.Infof("region %d split to left %d with size %d and right %d with size %d",
		oldRegion.Id, left.meta.Id, left.sizeHint, right.meta.Id, right.sizeHint)
	return nil
}

func (rm *RegionManager) Close() error {
	close(rm.closeCh)
	rm.wg.Wait()
	return nil
}
