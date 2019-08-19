package raftstore

import (
	"bytes"

	"github.com/coocood/badger"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/zhangjinpeng1987/raft"
)

type RegionChangeEvent int

const (
	RegionChangeEvent_Create RegionChangeEvent = 0 + iota
	RegionChangeEvent_Update
	RegionChangeEvent_Destroy
)

type coprocessor interface {
	start()
	stop()
}

type splitCheckObserver interface {
	coprocessor

	/// addChecker adds a checker for a split scan
	addChecker(_ *observerContext, _ *splitCheckerHost, _ *badger.DB, _ pdpb.CheckPolicy)
}

type observerContext struct {
	region *metapb.Region
	bypass bool
}

/// splitChecker is invoked during a split check scan, and decides to use
/// which keys to split a region.
type splitChecker interface {
	/// onKv is a hook called for every kv scanned during split.
	/// Return true to abort scan early.
	onKv(_ *observerContext, _ splitCheckKeyEntry) bool

	/// splitKeys returns the desired split keys.
	getSplitKeys() [][]byte

	/// approximateSplitKeys returns the split keys without scan.
	approximateSplitKeys(_ *metapb.Region, _ *badger.DB) ([][]byte, error)

	/// policy returns the policy.
	policy() pdpb.CheckPolicy
}

type sizeSplitChecker struct {
	maxSize         uint64
	splitSize       uint64
	currentSize     uint64
	splitKeys       [][]byte
	batchSplitLimit uint64
	checkPolicy     pdpb.CheckPolicy
}

func newSizeSplitChecker(maxSize, splitSize, batchSplitLimit uint64, policy pdpb.CheckPolicy) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:         maxSize,
		splitSize:       splitSize,
		batchSplitLimit: batchSplitLimit,
		checkPolicy:     policy,
	}
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func (checker *sizeSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	size := spCheckKeyEntry.entrySize()
	checker.currentSize += size
	overLimit := uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	if checker.currentSize > checker.splitSize && !overLimit {
		checker.splitKeys = append(checker.splitKeys, safeCopy(spCheckKeyEntry.key))
		// If for previous onKv(), checker.current_size == checker.split_size,
		// the split key would be pushed this time, but the entry size for this time should not be ignored.
		if checker.currentSize-size == checker.splitSize {
			checker.currentSize = size
		} else {
			checker.currentSize = 0
		}
		overLimit = uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	}
	// For a large region, scan over the range maybe cost too much time,
	// so limit the number of produced splitKeys for one batch.
	// Also need to scan over checker.maxSize for last part.
	return overLimit && checker.currentSize+checker.splitSize >= checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKeys() [][]byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize+checker.splitSize < checker.maxSize {
		splitKeyLen := len(checker.splitKeys)
		if splitKeyLen != 0 {
			checker.splitKeys = checker.splitKeys[:splitKeyLen-1]
		}
	}
	keys := checker.splitKeys
	checker.splitKeys = nil
	return keys
}

func getRegionApproximateSplitKeys(db *badger.DB, region *metapb.Region, splitSize, maxSize,
	batchSplitLimit uint64) ([][]byte, error) {
	// todo later, since badger db interface doesn't support approximate memtable stats right now,
	return nil, nil
}

func (checker *sizeSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	return getRegionApproximateSplitKeys(db, region, checker.splitSize, checker.maxSize, checker.batchSplitLimit)
}

func (checker *sizeSplitChecker) policy() pdpb.CheckPolicy {
	return checker.checkPolicy
}

type sizeSplitCheckObserver struct {
	regionMaxSize uint64
	splitSize     uint64
	splitLimit    uint64
	router        *router
}

func newSizeSplitCheckObserver(regionMaxSize, splitSize, splitLimit uint64, router *router) *sizeSplitCheckObserver {
	return &sizeSplitCheckObserver{
		regionMaxSize: regionMaxSize,
		splitSize:     splitSize,
		splitLimit:    splitLimit,
		router:        router,
	}
}

func (observer *sizeSplitCheckObserver) start() {}

func (observer *sizeSplitCheckObserver) stop() {}

func getRegionApproximateSize(db *badger.DB, region *metapb.Region) (uint64, error) {
	// todo later, since badger db interface doesn't support approximate memtable stats right now,
	return 0, nil
}

func (observer *sizeSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost,
	db *badger.DB, policy pdpb.CheckPolicy) {
	region := obCtx.region
	regionId := region.Id
	regionSize, err := getRegionApproximateSize(db, region)
	if err != nil {
		log.Warnf("failed to get approximate stat. [regionId: %d, err: %v]", regionId, err)
		host.addChecker(newSizeSplitChecker(observer.regionMaxSize, observer.splitSize, observer.splitLimit, policy))
		return
	}

	// Send it to raftstore to update region approximate size
	err = observer.router.send(regionId, NewPeerMsg(MsgTypeRegionApproximateSize, regionId, regionSize))
	if err != nil {
		log.Warnf("failed to send approximate region size. [regionId: %d, err: %v]", regionId, err)
	}
	if regionSize >= observer.regionMaxSize {
		log.Infof("approximate size over threshold, need to do split check. [regionId: %d, size : %d, threshold: %d]",
			regionId, regionSize, observer.regionMaxSize)
		// When meet large region use approximate way to produce split keys.
		if regionSize >= observer.regionMaxSize*observer.splitLimit*2 {
			policy = pdpb.CheckPolicy_APPROXIMATE
		}
		host.addChecker(newSizeSplitChecker(observer.regionMaxSize, observer.splitSize, observer.splitLimit, policy))
	} else {
		log.Debugf("approximate size less than threshold, doesn't need to do split check. [regionId: %d, size : %d, threshold : %d]",
			regionId, regionSize, observer.regionMaxSize)
	}
}

type halfSplitChecker struct {
	buckets        [][]byte
	curBucketSize  uint64
	eachBucketSize uint64
	checkPolicy    pdpb.CheckPolicy
}

func newHalfSplitCheck(eachBucketSize uint64, policy pdpb.CheckPolicy) *halfSplitChecker {
	return &halfSplitChecker{eachBucketSize: eachBucketSize, checkPolicy: policy}
}

func (checker *halfSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	if len(checker.buckets) == 0 || checker.curBucketSize >= checker.eachBucketSize {
		checker.buckets = append(checker.buckets, safeCopy(spCheckKeyEntry.key))
		checker.curBucketSize = 0
	}
	checker.curBucketSize += spCheckKeyEntry.entrySize()
	return false
}

func (checker *halfSplitChecker) getSplitKeys() [][]byte {
	bucketLen := len(checker.buckets)
	mid := bucketLen / 2
	if mid == 0 {
		return nil
	}
	dataKey := checker.buckets[mid]
	checker.buckets[mid] = checker.buckets[bucketLen-1]
	checker.buckets = checker.buckets[:bucketLen-1]
	return [][]byte{dataKey}
}

func getRegionApproximateMiddle(db *badger.DB, region *metapb.Region) ([]byte, error) {
	// todo later, since badger db interface doesn't support some operations here. like get table properties.
	return nil, nil
}

func (checker *halfSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	if keys, err := getRegionApproximateMiddle(db, region); err != nil {
		return nil, err
	} else {
		if keys == nil {
			return nil, nil
		}
		return [][]byte{keys}, nil
	}
}

func (checker *halfSplitChecker) policy() pdpb.CheckPolicy {
	return checker.checkPolicy
}

type halfSplitCheckObserver struct {
	halfSplitBucketSize uint64
}

const BucketNumberLimit uint64 = 1024
const BucketSizeLimitMb uint64 = 50

func newHalfSplitCheckObserver(regionSizeLimit uint64) *halfSplitCheckObserver {
	halfSplitBucketSize := regionSizeLimit / BucketNumberLimit
	bucketSizeLimit := MB * BucketSizeLimitMb
	if halfSplitBucketSize == 0 {
		halfSplitBucketSize = 1
	} else if halfSplitBucketSize > bucketSizeLimit {
		halfSplitBucketSize = bucketSizeLimit
	}
	return &halfSplitCheckObserver{halfSplitBucketSize: halfSplitBucketSize}
}

func (observer *halfSplitCheckObserver) start() {}

func (observer *halfSplitCheckObserver) stop() {}

func (observer *halfSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	if host.autoSplit {
		return
	}
	host.addChecker(newHalfSplitCheck(observer.halfSplitBucketSize, policy))
}

type keysSplitChecker struct {
	maxKeysCount    uint64
	splitThreshold  uint64
	currentCount    uint64
	splitKeys       [][]byte
	batchSplitLimit uint64
	checkPolicy     pdpb.CheckPolicy
}

func newKeysSplitChecker(maxKeysCount, splitThreshold, batchSplitLimit uint64,
	policy pdpb.CheckPolicy) *keysSplitChecker {
	return &keysSplitChecker{
		maxKeysCount:    maxKeysCount,
		splitThreshold:  splitThreshold,
		batchSplitLimit: batchSplitLimit,
		checkPolicy:     policy,
	}
}

func (checker *keysSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	checker.currentCount += 1
	overLimit := uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	if checker.currentCount > checker.splitThreshold && !overLimit {
		checker.splitKeys = append(checker.splitKeys, safeCopy(spCheckKeyEntry.key))
		// If for previous onKv(), checker.currentCount == checker.splitThreshold
		// the split key would be pushed this time, but the entry size for this time should not be ignored.
		checker.currentCount = 1
		overLimit = uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	}
	// For a large region, scan over the range maybe cost too much time,
	// so limit the number of produced splitKeys for one batch.
	// Also need to scan over checker.maxSize for last part.
	return overLimit && checker.currentCount+checker.splitThreshold >= checker.maxKeysCount
}

func (checker *keysSplitChecker) getSplitKeys() [][]byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentCount+checker.splitThreshold < checker.maxKeysCount {
		if len(checker.splitKeys) != 0 {
			checker.splitKeys = checker.splitKeys[:len(checker.splitKeys)-1]
		}
	}
	keys := checker.splitKeys
	checker.splitKeys = nil
	return keys
}

func (checker *keysSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	return nil, nil
}

func (checker *keysSplitChecker) policy() pdpb.CheckPolicy {
	return checker.checkPolicy
}

type keysSplitCheckObserver struct {
	regionMaxKeys   uint64
	splitKeys       uint64
	batchSplitLimit uint64
	router          *router
}

func newKeysSplitCheckObserver(regionMaxKeys, splitKeys, batchSplitLimit uint64,
	router *router) *keysSplitCheckObserver {
	return &keysSplitCheckObserver{
		regionMaxKeys:   regionMaxKeys,
		splitKeys:       splitKeys,
		batchSplitLimit: batchSplitLimit,
		router:          router,
	}
}

func (observer *keysSplitCheckObserver) start() {}

func (observer *keysSplitCheckObserver) stop() {}

func getRegionApproximateKeys(db *badger.DB, region *metapb.Region) (uint64, error) {
	// todo later, since badger doesn't support approximate memtable stats yet.
	return 0, nil
}

func (observer *keysSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	region := obCtx.region
	regionId := region.Id
	regionKeys, err := getRegionApproximateKeys(db, region)
	if err != nil {
		log.Warnf("failed to get approximate keys. [regionId: %d, err: %v]", regionId, err)
		host.addChecker(newKeysSplitChecker(observer.regionMaxKeys, observer.splitKeys, observer.batchSplitLimit, policy))
		return
	}
	err = observer.router.send(regionId, NewPeerMsg(MsgTypeRegionApproximateKeys, regionId, regionKeys))
	if err != nil {
		log.Warnf("failed to send approximate region keys. [regionId: %d, err: %v]", regionId, err)
	}
	if regionKeys >= observer.regionMaxKeys {
		log.Infof("approximate keys over threshold, need to do split check. [regionId: %d, keys: %d, threshold: %d",
			regionId, regionKeys, observer.regionMaxKeys)
		// Need to check keys.
		host.addChecker(newKeysSplitChecker(observer.regionMaxKeys, observer.splitKeys, observer.batchSplitLimit, policy))
	} else {
		log.Debugf("approximate keys less than threshold, doesn't need to do split check. [regionId: %d, keys: %d, threshold: %d]",
			regionId, regionKeys, observer.regionMaxKeys)
	}
}

type tableSplitChecker struct {
	splitKey    []byte
	checkPolicy pdpb.CheckPolicy
}

func newTableSplitChecker(splitKey []byte, policy pdpb.CheckPolicy) *tableSplitChecker {
	return &tableSplitChecker{
		splitKey:    splitKey,
		checkPolicy: policy,
	}
}

func isTableKey(encodedKey []byte) bool {
	return bytes.HasPrefix(encodedKey, tablecodec.TablePrefix())
}

func isSameTable(leftKey, rightKey []byte) bool {
	return bytes.HasPrefix(leftKey, tablecodec.TablePrefix()) &&
		bytes.HasPrefix(rightKey, tablecodec.TablePrefix()) &&
		len(leftKey) >= tablecodec.TableSplitKeyLen &&
		len(rightKey) >= tablecodec.TableSplitKeyLen &&
		bytes.Compare(leftKey[:tablecodec.TableSplitKeyLen], rightKey[:tablecodec.TableSplitKeyLen]) == 0
}

func extractTablePrefix(key []byte) []byte {
	return key[:tablecodec.TableSplitKeyLen]
}

// Feed keys in order to find the split key.
func (checker *tableSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	if len(checker.splitKey) != 0 {
		return true
	}
	if isTableKey(spCheckKeyEntry.key) {
		checker.splitKey = extractTablePrefix(spCheckKeyEntry.key)
		return true
	}
	return false
}

func (checker *tableSplitChecker) getSplitKeys() [][]byte {
	if len(checker.splitKey) == 0 {
		return nil
	}
	return [][]byte{checker.splitKey}
}

func (checker *tableSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	return nil, nil
}

func (checker *tableSplitChecker) policy() pdpb.CheckPolicy {
	return checker.checkPolicy
}

type tableSplitCheckObserver struct{}

func (observer *tableSplitCheckObserver) start() {}

func (observer *tableSplitCheckObserver) stop() {}

func lastKeyOfRegion(db *badger.DB, regionStartKey, regionEndKey []byte) []byte {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	ite := dbreader.NewIterator(txn, true, regionStartKey, regionEndKey)
	defer ite.Close()
	if ite.Seek(regionEndKey); ite.Valid() {
		item := ite.Item()
		if bytes.Compare(item.Key(), regionEndKey) == 0 {
			if ite.Next(); ite.Valid() {
				item = ite.Item()
			} else {
				return nil
			}
		}
		if bytes.Compare(item.Key(), regionStartKey) >= 0 {
			return item.KeyCopy(nil)
		}
	}
	return nil
}

func (observer *tableSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	region := obCtx.region
	_, regionStartKey, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		return
	}
	_, regionEndKey, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		return
	}
	if !isTableKey(regionStartKey) {
		return
	}
	if isSameTable(regionStartKey, regionEndKey) {
		// Region is inside a table, skip for saving IO.
		return
	}
	lastKey := lastKeyOfRegion(db, regionStartKey, regionEndKey)
	if len(lastKey) == 0 {
		return
	}
	if isSameTable(regionStartKey, lastKey) {
		// Same table
		return
	} else {
		// Different tables.
		// Note that table id does not grow by 1, so have to use endKey to extract a table prefix.
		// See more: https://github.com/pingcap/tidb/issues/4727.
		splitKey := extractTablePrefix(lastKey)
		host.addChecker(newTableSplitChecker(splitKey, policy))
	}
}

type splitCheckerHost struct {
	autoSplit bool
	checkers  []splitChecker
}

func (host *splitCheckerHost) skip() bool {
	return len(host.checkers) == 0
}

/// onKv is a hook called for every check during split.
/// Return true means abort early.
func (host *splitCheckerHost) onKv(region *metapb.Region, spCheKeyEntry splitCheckKeyEntry) bool {
	obCtx := &observerContext{region: region}
	for _, checker := range host.checkers {
		if checker.onKv(obCtx, spCheKeyEntry) {
			return true
		}
	}
	return false
}

func (host *splitCheckerHost) splitKeys() [][]byte {
	for _, checker := range host.checkers {
		keys := checker.getSplitKeys()
		if len(keys) != 0 {
			return keys
		}
	}
	return nil
}

func (host *splitCheckerHost) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	for _, checker := range host.checkers {
		keys, err := checker.approximateSplitKeys(region, db)
		if err != nil {
			return nil, err
		}
		if len(keys) != 0 {
			return keys, nil
		}
	}
	return nil, nil
}

func (host *splitCheckerHost) policy() pdpb.CheckPolicy {
	for _, checker := range host.checkers {
		if checker.policy() == pdpb.CheckPolicy_APPROXIMATE {
			return pdpb.CheckPolicy_APPROXIMATE
		}
	}
	return pdpb.CheckPolicy_SCAN
}

func (host *splitCheckerHost) addChecker(checker splitChecker) {
	host.checkers = append(host.checkers, checker)
}

type splitCheckConfig struct {

	// When it is true, it will try to split a region with table prefix if
	// that region crosses tables.
	splitRegionOnTable bool

	// For once split check, there are several splitKey produced for batch.
	// batchSplitLimit limits the number of produced split-key for one batch.
	batchSplitLimit uint64

	// When region [a,e) size meets regionMaxSize, it will be split into
	// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
	// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
	regionMaxSize   uint64
	regionSplitSize uint64

	// When the number of keys in region [a,e) meets the region_max_keys,
	// it will be split into two several regions [a,b), [b,c), [c,d), [d,e).
	// And the number of keys in [a,b), [b,c), [c,d) will be region_split_keys.
	regionMaxKeys   uint64
	regionSplitKeys uint64
}

const (
	// Default region split size.
	splitSizeMB uint64 = 96
	// Default region split keys.
	splitKeys uint64 = 960000
	// Default batch split limit.
	batchSplitLimit uint64 = 10
)

func newDefaultSplitCheckConfig() *splitCheckConfig {
	splitSize := splitSizeMB * MB
	return &splitCheckConfig{
		splitRegionOnTable: true,
		batchSplitLimit:    batchSplitLimit,
		regionSplitSize:    splitSize,
		regionMaxSize:      splitSize / 2 * 3,
		regionSplitKeys:    splitKeys,
		regionMaxKeys:      splitKeys / 2 * 3,
	}
}

type registry struct {
	splitCheckObservers []splitCheckObserver
}

type CoprocessorHost struct {
	// Todo: currently it is a place holder
	registry registry
}

func newCoprocessorHost(config *splitCheckConfig, router *router) *CoprocessorHost {
	host := &CoprocessorHost{}
	// NOTE: the split check observer order is hard coded.
	halfSplitCheckObserver := newHalfSplitCheckObserver(config.regionMaxSize)

	sizeSplitCheckObserver := newSizeSplitCheckObserver(config.regionMaxSize, config.regionSplitSize,
		config.batchSplitLimit, router)

	keysSplitCheckObserver := newKeysSplitCheckObserver(config.regionMaxKeys, config.regionSplitKeys,
		config.batchSplitLimit, router)

	// TableCheckObserver has higher priority than SizeCheckObserver.
	host.registry.splitCheckObservers = append(host.registry.splitCheckObservers, halfSplitCheckObserver,
		sizeSplitCheckObserver, keysSplitCheckObserver)
	if config.splitRegionOnTable {
		host.registry.splitCheckObservers = append(host.registry.splitCheckObservers, &tableSplitCheckObserver{})
	}

	return host
}

func (c *CoprocessorHost) PrePropose(region *metapb.Region, req *raft_cmdpb.RaftCmdRequest) error {
	// Todo: currently it is a place holder
	return nil
}

func (c *CoprocessorHost) OnRegionChanged(region *metapb.Region, event RegionChangeEvent, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) OnRoleChanged(region *metapb.Region, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) newSplitCheckerHost(region *metapb.Region, engine *badger.DB, autoSplit bool,
	policy pdpb.CheckPolicy) *splitCheckerHost {
	host := &splitCheckerHost{autoSplit: autoSplit}
	ctx := &observerContext{region: region}
	for _, observer := range c.registry.splitCheckObservers {
		observer.addChecker(ctx, host, engine, policy)
		if ctx.bypass {
			break
		}
	}
	return host
}

func (c *CoprocessorHost) preApply(region *metapb.Region, req *raft_cmdpb.RaftCmdRequest) {
	// TODO: placeholder
}

func (c *CoprocessorHost) postApply(region *metapb.Region, resp *raft_cmdpb.RaftCmdResponse) {
	// TODO: placeholder
}

func (c *CoprocessorHost) shutdown() {}
