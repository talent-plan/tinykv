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
	splitKeys() [][]byte

	/// approximateSplitKeys returns the split keys without scan.
	approximateSplitKeys(_ *metapb.Region, _ *badger.DB) ([][]byte, error)

	/// policy returns the policy.
	policy() pdpb.CheckPolicy
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
		keys := checker.splitKeys()
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
