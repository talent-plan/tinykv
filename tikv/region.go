package tikv

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"encoding/binary"
	"github.com/dgraph-io/badger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
)

var (
	InternalKeyPrefix        = []byte(`i`)
	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
)

func InternalRegionMetaKey(regionId uint64) []byte {
	return []byte(string(InternalRegionMetaPrefix) + strconv.FormatUint(regionId, 10))
}

type regionInfo struct {
	meta       *metapb.Region
	sizeHint   int64
	diff       int64
	lastUpdate uint64
}

func (ri *regionInfo) rawStartKey() []byte {
	if len(ri.meta.StartKey) == 0 {
		return nil
	}
	rawKey, _, err := mvDecode(ri.meta.StartKey, nil)
	if err != nil {
		panic("invalid region start key")
	}
	return rawKey
}

func (ri *regionInfo) rawEndKey() []byte {
	if len(ri.meta.EndKey) == 0 {
		return nil
	}
	rawKey, _, err := mvDecode(ri.meta.EndKey, nil)
	if err != nil {
		panic("invalid region end key")
	}
	return rawKey
}

func (ri *regionInfo) assertContainsKey(rawKey []byte) {
	var keyBuf [128]byte
	mvKey := codec.EncodeBytes(keyBuf[:0], rawKey)
	if ri.lessThanStartKey(mvKey) || ri.greaterEqualEndKey(mvKey) {
		tid, handle, er := tablecodec.DecodeRecordKey(rawKey)
		log.Error(tid, handle, er)
		panic(fmt.Sprintf("key %q not in region %s", mvKey, ri.meta))
	}
}

func (ri *regionInfo) lessThanStartKey(mvKey []byte) bool {
	return bytes.Compare(mvKey, ri.meta.StartKey) < 0
}

func (ri *regionInfo) greaterEqualEndKey(mvKey []byte) bool {
	return len(ri.meta.EndKey) > 0 && bytes.Compare(mvKey, ri.meta.EndKey) >= 0
}

func (ri *regionInfo) greaterThanEndKey(mvKey []byte) bool {
	return len(ri.meta.EndKey) > 0 && bytes.Compare(mvKey, ri.meta.EndKey) > 0
}

func (ri *regionInfo) assertContainsRange(r *coprocessor.KeyRange) {
	ri.assertContainsKey(r.Start)
	var keyBuf [128]byte
	mvEndKey := codec.EncodeBytes(keyBuf[:0], r.End)
	if ri.greaterThanEndKey(mvEndKey) {
		panic(fmt.Sprintf("end key %q not in region %s", r.End, ri.meta))
	}
}

func (ri *regionInfo) unmarshal(data []byte) error {
	ri.sizeHint = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	ri.meta = &metapb.Region{}
	return ri.meta.Unmarshal(data)
}

func (ri *regionInfo) marshal() []byte {
	data := make([]byte, 8+ri.meta.Size())
	binary.LittleEndian.PutUint64(data, uint64(ri.sizeHint))
	_, err := ri.meta.MarshalTo(data[8:])
	if err != nil {
		log.Error(err)
	}
	return data
}

type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

type RegionManager struct {
	storeMeta  metapb.Store
	mu         sync.RWMutex
	regions    map[uint64]*regionInfo
	db         *badger.DB
	pdc        Client
	clusterID  uint64
	regionSize int64
}

func NewRegionManager(db *badger.DB, opts RegionOptions) *RegionManager {
	pdc, err := NewClient(opts.PDAddr, "")
	if err != nil {
		log.Fatal(err)
	}
	clusterID := pdc.GetClusterID(context.TODO())
	log.Infof("cluster id %v", clusterID)
	rm := &RegionManager{
		db:         db,
		pdc:        pdc,
		clusterID:  clusterID,
		regions:    make(map[uint64]*regionInfo),
		regionSize: opts.RegionSize,
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
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := InternalRegionMetaPrefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err1 = item.Value()
			if err1 != nil {
				return err1
			}
			r := &regionInfo{}
			err = r.unmarshal(val)
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
	go rm.runSplitWorker()
	go rm.storeHeartBeatLoop()
	return rm
}

func (rm *RegionManager) initStore(storeAddr string) error {
	log.Info("initializing store")
	// allocate store id
	storeID, err := rm.pdc.AllocID(context.Background())
	if err != nil {
		return err
	}
	rm.storeMeta.Id = storeID
	rm.storeMeta.Address = storeAddr

	// allocate retion id
	rid, err := rm.pdc.AllocID(context.Background())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	rootRegion := &metapb.Region{
		Id:          rid,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{&metapb.Peer{Id: rid, StoreId: rm.storeMeta.Id}},
	}
	rm.regions[rootRegion.Id] = &regionInfo{meta: rootRegion}
	err = rm.pdc.Bootstrap(ctx, &rm.storeMeta, rootRegion)
	cancel()
	if err != nil {
		log.Fatal("Initialize failed: ", err)
	} else {
		log.Info("Initialize success")
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
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

func (rm *RegionManager) storeHeartBeatLoop() {
	ticker := time.Tick(time.Second * 3)
	for {
		<-ticker
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

func (rm *RegionManager) getRegionFromCtx(ctx *kvrpcpb.Context) (*regionInfo, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != rm.storeMeta.Id {
		return nil, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	rm.mu.RLock()
	ri := rm.regions[ctx.RegionId]
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
	if *ri.meta.GetRegionEpoch() != *ctx.GetRegionEpoch() {
		return nil, &errorpb.Error{
			Message: "stale epoch",
			StaleEpoch: &errorpb.StaleEpoch{
				NewRegions: []*metapb.Region{ri.meta},
			},
		}
	}
	return ri, nil
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
			// remove the last 8 bytes to make a key with all its version in one region.
			return sample.key[:len(sample.key)-8], sample.leftSize
		}
	}
	return []byte{}, 0
}

func (rm *RegionManager) runSplitWorker() {
	var regionsToCheck []*regionInfo
	var regionsToSave []*regionInfo
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

		time.Sleep(time.Second * 5)
	}
}

func (rm *RegionManager) saveSizeHint(regionsToSave []*regionInfo) {
	err1 := rm.db.Update(func(txn *badger.Txn) error {
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

func (rm *RegionManager) splitCheckRegion(region *regionInfo) error {
	s := newSampler()
	err := rm.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer iter.Close()
		for iter.Seek(region.meta.StartKey); iter.Valid(); iter.Next() {
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
	err = rm.splitRegion(region.meta, splitKey, s.totalSize, leftSize)
	if err != nil {
		log.Error(err)
	}
	return errors.Trace(err)
}

func (rm *RegionManager) splitRegion(oldRegion *metapb.Region, splitKey []byte, oldSize, leftSize int64) error {
	rightMeta := &metapb.Region{
		Id:       oldRegion.Id,
		StartKey: splitKey,
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version + 1,
		},
		Peers: oldRegion.Peers,
	}
	right := &regionInfo{meta: rightMeta, sizeHint: oldSize - leftSize}
	id, err := rm.pdc.AllocID(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	leftMeta := &metapb.Region{
		Id:       id,
		StartKey: oldRegion.StartKey,
		EndKey:   splitKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: oldRegion.Peers,
	}
	left := &regionInfo{meta: leftMeta, sizeHint: leftSize}
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
	rm.pdc.ReportRegion(right)
	rm.pdc.ReportRegion(left)
	log.Infof("region %d split to left %d with size %d and right %d with size %d",
		oldRegion.Id, left.meta.Id, left.sizeHint, right.meta.Id, right.sizeHint)
	return nil
}
