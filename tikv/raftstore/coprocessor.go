package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
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

func (checker *sizeSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	//Todo, currently it is a place holder
	return false
}

func (checker *sizeSplitChecker) getSplitKeys() [][]byte {
	//Todo, currently it is a place holder
	return nil
}

func (checker *sizeSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	//Todo, currently it is a place holder
	return nil, nil
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

func (observer *sizeSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost,
	db *badger.DB, policy pdpb.CheckPolicy) {
	//todo, currently it is just a placeholder
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
	//Todo, currently it is a place holder
	return false
}

func (checker *halfSplitChecker) getSplitKeys() [][]byte {
	//Todo, currently it is a place holder
	return nil
}

func (checker *halfSplitChecker) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	//Todo, currently it is a place holder
	return nil, nil
}

func (checker *halfSplitChecker) policy() pdpb.CheckPolicy {
	return checker.checkPolicy
}

type halfSplitCheckObserver struct {
	halfSplitBucketSize uint64
}

func newHalfSplitCheckObserver(regionSizeLimit uint64) *halfSplitCheckObserver {
	//todo, currently it is a place holder
	return nil
}

func (observer *halfSplitCheckObserver) start() {}

func (observer *halfSplitCheckObserver) stop() {}

func (observer *halfSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	//todo, currently it is just a placeholder
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
	//Todo, currently it is a place holder
	return false
}

func (checker *keysSplitChecker) getSplitKeys() [][]byte {
	//Todo, currently it is a place holder
	return nil
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

func (observer *keysSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	//todo, currently it is just a placeholder
}

type tableSplitChecker struct {
	firstEncodedTablePrefix []byte
	splitKey                []byte
	checkPolicy             pdpb.CheckPolicy
}

func newTableSplitChecker(firstEncodedTablePrefix, splitKey []byte, policy pdpb.CheckPolicy) *tableSplitChecker {
	return &tableSplitChecker{
		firstEncodedTablePrefix: firstEncodedTablePrefix,
		splitKey:                splitKey,
		checkPolicy:             policy,
	}
}

func (checker *tableSplitChecker) onKv(obCtx *observerContext, spCheckKeyEntry splitCheckKeyEntry) bool {
	//Todo, currently it is a place holder
	return false
}

func (checker *tableSplitChecker) getSplitKeys() [][]byte {
	//Todo, currently it is a place holder
	return nil
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

func (observer *tableSplitCheckObserver) addChecker(obCtx *observerContext, host *splitCheckerHost, db *badger.DB,
	policy pdpb.CheckPolicy) {
	//todo, currently it is just a placeholder
}

type splitCheckerHost struct {
	autoSplit bool
	checkers  []splitChecker
}

func (spCheckerHost *splitCheckerHost) skip() bool {
	return len(spCheckerHost.checkers) == 0
}

/// onKv is a hook called for every check during split.
/// Return true means abort early.
func (spCheckerHost *splitCheckerHost) onKv(region *metapb.Region, spCheKeyEntry splitCheckKeyEntry) bool {
	obCtx := &observerContext{region: region}
	for _, checker := range spCheckerHost.checkers {
		if checker.onKv(obCtx, spCheKeyEntry) {
			return true
		}
	}
	return false
}

func (spCheckerHost *splitCheckerHost) splitKeys() [][]byte {
	for _, checker := range spCheckerHost.checkers {
		keys := checker.getSplitKeys()
		if len(keys) != 0 {
			return keys
		}
	}
	return nil
}

func (spCheckerHost *splitCheckerHost) approximateSplitKeys(region *metapb.Region, db *badger.DB) ([][]byte, error) {
	for _, checker := range spCheckerHost.checkers {
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

func (spCheckerHost *splitCheckerHost) policy() pdpb.CheckPolicy {
	// Todo, currently it is a place holder
	return 0
}

type registry struct {
	splitCheckObservers []splitCheckObserver
}

type CoprocessorHost struct {
	// Todo: currently it is a place holder
	registry registry
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
	for _, server := range c.registry.splitCheckObservers {
		server.addChecker(ctx, host, engine, policy)
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
