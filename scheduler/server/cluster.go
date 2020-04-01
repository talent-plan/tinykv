// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	backgroundJobInterval      = time.Minute
	defaultChangedRegionsLimit = 10000
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex
	ctx context.Context

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	meta    *metapb.Cluster
	opt     *config.ScheduleOption
	storage *core.Storage
	id      id.Allocator

	coordinator *coordinator

	wg   sync.WaitGroup
	quit chan struct{}
}

// ClusterStatus saves some state information
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
}

func newRaftCluster(ctx context.Context, s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		ctx:         ctx,
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) loadClusterStatus() (*ClusterStatus, error) {
	bootstrapTime, err := c.loadBootstrapTime()
	if err != nil {
		return nil, err
	}
	var isInitialized bool
	if bootstrapTime != typeutil.ZeroTime {
		isInitialized = c.isInitialized()
	}
	return &ClusterStatus{
		RaftBootstrapTime: bootstrapTime,
		IsInitialized:     isInitialized,
	}, nil
}

func (c *RaftCluster) isInitialized() bool {
	if c.core.GetRegionCount() > 1 {
		return true
	}
	region := c.core.SearchRegion(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.s.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped
// yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.s.storage.Load(c.s.storage.ClusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

func (c *RaftCluster) initCluster(id id.Allocator, opt *config.ScheduleOption, storage *core.Storage) {
	c.core = core.NewBasicCluster()
	c.opt = opt
	c.storage = storage
	c.id = id
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.initCluster(c.s.idAllocator, c.s.scheduleOpt, c.s.storage)
	cluster, err := c.loadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	c.coordinator = newCoordinator(c.ctx, cluster, c.s.hbStreams)
	c.quit = make(chan struct{})

	c.wg.Add(2)
	go c.runCoordinator()
	go c.runBackgroundJobs(backgroundJobInterval)
	c.running = true

	return nil
}

// Return nil if cluster is not bootstrapped.
func (c *RaftCluster) loadClusterInfo() (*RaftCluster, error) {
	c.meta = &metapb.Cluster{}
	ok, err := c.storage.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := c.storage.LoadStores(c.core.PutStore); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.getStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			log.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		log.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator is stopping")
}

func (c *RaftCluster) stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetHeartbeatStreams returns the heartbeat streams.
func (c *RaftCluster) GetHeartbeatStreams() *heartbeatStreams {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.hbStreams
}

// GetCoordinator returns the coordinator.
func (c *RaftCluster) GetCoordinator() *coordinator {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator
}

// handleStoreHeartbeat updates the store status.
func (c *RaftCluster) handleStoreHeartbeat(stats *schedulerpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	c.core.PutStore(newStore)
	return nil
}

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	c.RLock()
	origin := c.GetRegion(region.GetID())
	if origin == nil {
		for _, item := range c.core.GetOverlaps(region) {
			if region.GetRegionEpoch().GetVersion() < item.GetRegionEpoch().GetVersion() {
				c.RUnlock()
				return ErrRegionIsStale(region.GetMeta(), item.GetMeta())
			}
		}
	}
	c.RUnlock()

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveCache bool
	if origin == nil {
		log.Debug("insert new region",
			zap.Uint64("region-id", region.GetID()),
			zap.Stringer("meta-region", core.RegionToHexMeta(region.GetMeta())),
		)
		saveCache = true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		// Region meta is stale, return an error.
		if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
			return ErrRegionIsStale(region.GetMeta(), origin.GetMeta())
		}
		if r.GetVersion() > o.GetVersion() {
			log.Info("region Version changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionKeyInfo(origin, region)),
				zap.Uint64("old-version", o.GetVersion()),
				zap.Uint64("new-version", r.GetVersion()),
			)
			saveCache = true
		}
		if r.GetConfVer() > o.GetConfVer() {
			log.Info("region ConfVer changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionPeersInfo(origin, region)),
				zap.Uint64("old-confver", o.GetConfVer()),
				zap.Uint64("new-confver", r.GetConfVer()),
			)
			saveCache = true
		}
		if region.GetLeader().GetId() != origin.GetLeader().GetId() {
			if origin.GetLeader().GetId() != 0 {
				log.Info("leader changed",
					zap.Uint64("region-id", region.GetID()),
					zap.Uint64("from", origin.GetLeader().GetStoreId()),
					zap.Uint64("to", region.GetLeader().GetStoreId()),
				)
			}
			saveCache = true
		}
		if len(region.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(origin.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(region.GetPeers()) != len(origin.GetPeers()) {
			saveCache = true
		}

		if region.GetApproximateSize() != origin.GetApproximateSize() {
			saveCache = true
		}
	}

	if saveCache {
		c.Lock()
		defer c.Unlock()

		c.core.PutRegion(region)

		// Update related stores.
		if origin != nil {
			for _, p := range origin.GetPeers() {
				c.updateStoreStatusLocked(p.GetStoreId())
			}
		}
		for _, p := range region.GetPeers() {
			c.updateStoreStatusLocked(p.GetStoreId())
		}
	}

	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.GetStoreLeaderCount(id)
	regionCount := c.core.GetStoreRegionCount(id)
	pendingPeerCount := c.core.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.GetStoreLeaderRegionSize(id)
	regionSize := c.core.GetStoreRegionSize(id)
	c.core.UpdateStoreStatus(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *schedulerpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	return nil
}

func (c *RaftCluster) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *RaftCluster) putMetaLocked(meta *metapb.Cluster) error {
	if c.storage != nil {
		if err := c.storage.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetRegionByKey gets region and leader peer by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchPrevRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	return c.core.SearchRegion(regionKey)
}

// ScanRegions scans region with start key, until the region contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	return c.core.ScanRange(startKey, endKey, limit)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegion searches for a region by ID.
func (c *RaftCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return c.core.GetRegion(regionID)
}

// GetMetaRegions gets regions from cluster.
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	return c.core.GetMetaRegions()
}

// GetRegions returns all regions' information in detail.
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	return c.core.GetRegions()
}

// GetRegionCount returns total count of regions
func (c *RaftCluster) GetRegionCount() int {
	return c.core.GetRegionCount()
}

// GetStoreRegions returns all regions' information with a given storeID.
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	return c.core.GetStoreRegions(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *RaftCluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandLeaderRegion(storeID, opts...)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *RaftCluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandFollowerRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (c *RaftCluster) RandPendingRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandPendingRegion(storeID, opts...)
}

// GetPendingRegionsWithLock return pending regions subtree by storeID
func (c *RaftCluster) GetPendingRegionsWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetPendingRegionsWithLock(storeID, callback)
}

// GetLeadersWithLock return leaders subtree by storeID
func (c *RaftCluster) GetLeadersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetLeadersWithLock(storeID, callback)
}

// GetFollowersWithLock return leaders subtree by storeID
func (c *RaftCluster) GetFollowersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetFollowersWithLock(storeID, callback)
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (c *RaftCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return c.core.GetLeaderStore(region)
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (c *RaftCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetFollowerStores(region)
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *RaftCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetRegionStores(region)
}

func (c *RaftCluster) getStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreRegionCount returns the number of regions for a given store.
func (c *RaftCluster) GetStoreRegionCount(storeID uint64) int {
	return c.core.GetStoreRegionCount(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (c *RaftCluster) GetAverageRegionSize() int64 {
	return c.core.GetAverageRegionSize()
}

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if region := c.GetRegion(id); region != nil {
		c.core.RemoveRegion(region)
	}
}

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	return c.core.GetStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	return c.core.GetStore(storeID)
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	// Store address can not be the same as other stores.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetID() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.GetMeta())
		}
	}

	s := c.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Update an existed store.
		s = s.Clone(
			core.SetStoreAddress(store.Address),
		)
	}
	return c.putStoreLocked(s)
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	op := errcode.Op("store.remove")
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return op.AddTo(core.NewStoreNotFoundErr(storeID))
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return op.AddTo(core.StoreTombstonedErr{StoreID: storeID})
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	log.Warn("store has been offline",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error { // revive:disable-line:flag-parameter
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warn("forcedly bury store", zap.Stringer("store", store.GetMeta()))
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BlockStore stops balancer from selecting the store.
func (c *RaftCluster) BlockStore(storeID uint64) error {
	return c.core.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (c *RaftCluster) UnblockStore(storeID uint64) {
	c.core.UnblockStore(storeID)
}

// AttachAvailableFunc attaches an available function to a specific store.
func (c *RaftCluster) AttachAvailableFunc(storeID uint64, f func() bool) {
	c.core.AttachAvailableFunc(storeID, f)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	newStore := store.Clone(core.SetStoreState(state))
	log.Warn("store update state",
		zap.Uint64("store-id", storeID),
		zap.Stringer("new-state", state))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	if err := c.s.storage.SaveStoreWeight(storeID, leaderWeight, regionWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()
	for _, store := range stores {
		// the store has already been tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			upStoreCount++
			continue
		}

		offlineStore := store.GetMeta()
		// If the store is empty, it can be buried.
		regionCount := c.core.GetStoreRegionCount(offlineStore.GetId())
		if regionCount == 0 {
			if err := c.BuryStore(offlineStore.GetId(), false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", offlineStore),
					zap.Error(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	if upStoreCount < c.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	for _, store := range c.GetStores() {
		if store.IsTombstone() {
			// the store has already been tombstone
			err := c.deleteStoreLocked(store)
			if err != nil {
				log.Error("delete store failed",
					zap.Stringer("store", store.GetMeta()),
					zap.Error(err))
				return err
			}
			log.Info("delete store successed",
				zap.Stringer("store", store.GetMeta()))
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	return nil
}

func (c *RaftCluster) collectHealthStatus() {
	client := c.s.GetClient()
	members, err := GetMembers(client)
	if err != nil {
		log.Error("get members error", zap.Error(err))
	}
	unhealth := c.s.CheckHealth(members)
	for _, member := range members {
		if _, ok := unhealth[member.GetMemberId()]; ok {
			continue
		}
	}
}

func (c *RaftCluster) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (c *RaftCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetOpt returns the scheduling options.
func (c *RaftCluster) GetOpt() *config.ScheduleOption {
	return c.opt
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (c *RaftCluster) GetLeaderScheduleLimit() uint64 {
	return c.opt.GetLeaderScheduleLimit()
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (c *RaftCluster) GetRegionScheduleLimit() uint64 {
	return c.opt.GetRegionScheduleLimit()
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (c *RaftCluster) GetReplicaScheduleLimit() uint64 {
	return c.opt.GetReplicaScheduleLimit()
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (c *RaftCluster) GetPatrolRegionInterval() time.Duration {
	return c.opt.GetPatrolRegionInterval()
}

// GetMaxStoreDownTime returns the max down time of a store.
func (c *RaftCluster) GetMaxStoreDownTime() time.Duration {
	return c.opt.GetMaxStoreDownTime()
}

// GetMaxReplicas returns the number of replicas.
func (c *RaftCluster) GetMaxReplicas() int {
	return c.opt.GetMaxReplicas()
}

func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	c.core.PutRegion(region)
	return nil
}
