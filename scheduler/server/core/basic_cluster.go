// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	sync.RWMutex
	Stores  *StoresInfo
	Regions *RegionsInfo
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores:  NewStoresInfo(),
		Regions: NewRegionsInfo(),
	}
}

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStores()
}

// GetMetaStores gets a complete set of metapb.Store.
func (bc *BasicCluster) GetMetaStores() []*metapb.Store {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetMetaStores()
}

// GetStore searches for a store by ID.
func (bc *BasicCluster) GetStore(storeID uint64) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(storeID)
}

// GetRegion searches for a region by ID.
func (bc *BasicCluster) GetRegion(regionID uint64) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegion(regionID)
}

// GetRegions gets all RegionInfo from regionMap.
func (bc *BasicCluster) GetRegions() []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegions()
}

// GetMetaRegions gets a set of metapb.Region from regionMap.
func (bc *BasicCluster) GetMetaRegions() []*metapb.Region {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetMetaRegions()
}

// GetStoreRegions gets all RegionInfo with a given storeID.
func (bc *BasicCluster) GetStoreRegions(storeID uint64) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreRegions(storeID)
}

// GetRegionStores returns all Stores that contains the region's peer.
func (bc *BasicCluster) GetRegionStores(region *RegionInfo) []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetStoreIds() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (bc *BasicCluster) GetFollowerStores(region *RegionInfo) []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetFollowers() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetLeaderStore returns all Stores that contains the region's leader peer.
func (bc *BasicCluster) GetLeaderStore(region *RegionInfo) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

// BlockStore stops balancer from selecting the store.
func (bc *BasicCluster) BlockStore(storeID uint64) error {
	bc.Lock()
	defer bc.Unlock()
	return bc.Stores.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (bc *BasicCluster) UnblockStore(storeID uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.UnblockStore(storeID)
}

// AttachAvailableFunc attaches an available function to a specific store.
func (bc *BasicCluster) AttachAvailableFunc(storeID uint64, f func() bool) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.AttachAvailableFunc(storeID, f)
}

// UpdateStoreStatus updates the information of the store.
func (bc *BasicCluster) UpdateStoreStatus(storeID uint64, leaderCount int, regionCount int, pendingPeerCount int, leaderSize int64, regionSize int64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.UpdateStoreStatus(storeID, leaderCount, regionCount, pendingPeerCount, leaderSize, regionSize)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (bc *BasicCluster) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandFollowerRegion(storeID, opts...)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (bc *BasicCluster) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandLeaderRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (bc *BasicCluster) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandPendingRegion(storeID, opts...)
}

// GetPendingRegionsWithLock return pending regions subtree by storeID
func (bc *BasicCluster) GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.pendingPeers[storeID])
}

// GetLeadersWithLock return leaders subtree by storeID
func (bc *BasicCluster) GetLeadersWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.leaders[storeID])
}

// GetFollowersWithLock return leaders subtree by storeID
func (bc *BasicCluster) GetFollowersWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.followers[storeID])
}

// GetRegionCount gets the total count of RegionInfo of regionMap.
func (bc *BasicCluster) GetRegionCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegionCount()
}

// GetStoreCount returns the total count of storeInfo.
func (bc *BasicCluster) GetStoreCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStoreCount()
}

// GetStoreRegionCount gets the total count of a store's leader and follower RegionInfo by storeID.
func (bc *BasicCluster) GetStoreRegionCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID) + bc.Regions.GetStoreFollowerCount(storeID) + bc.Regions.GetStoreLearnerCount(storeID)
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo.
func (bc *BasicCluster) GetStoreLeaderCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID)
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo.
func (bc *BasicCluster) GetStoreFollowerCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreFollowerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer.
func (bc *BasicCluster) GetStorePendingPeerCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStorePendingPeerCount(storeID)
}

// GetStoreLeaderRegionSize get total size of store's leader regions.
func (bc *BasicCluster) GetStoreLeaderRegionSize(storeID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderRegionSize(storeID)
}

// GetStoreRegionSize get total size of store's regions.
func (bc *BasicCluster) GetStoreRegionSize(storeID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderRegionSize(storeID) + bc.Regions.GetStoreFollowerRegionSize(storeID) + bc.Regions.GetStoreLearnerRegionSize(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (bc *BasicCluster) GetAverageRegionSize() int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetAverageRegionSize()
}

// PutStore put a store.
func (bc *BasicCluster) PutStore(store *StoreInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.SetStore(store)
}

// DeleteStore deletes a store.
func (bc *BasicCluster) DeleteStore(store *StoreInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.DeleteStore(store)
}

// TakeStore returns the point of the origin StoreInfo with the specified storeID.
func (bc *BasicCluster) TakeStore(storeID uint64) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.TakeStore(storeID)
}

// PutRegion put a region.
func (bc *BasicCluster) PutRegion(region *RegionInfo) []*RegionInfo {
	bc.Lock()
	defer bc.Unlock()
	return bc.Regions.SetRegion(region)
}

// RemoveRegion removes RegionInfo from regionTree and regionMap.
func (bc *BasicCluster) RemoveRegion(region *RegionInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Regions.RemoveRegion(region)
}

// SearchRegion searches RegionInfo from regionTree.
func (bc *BasicCluster) SearchRegion(regionKey []byte) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.SearchRegion(regionKey)
}

// SearchPrevRegion searches previous RegionInfo from regionTree.
func (bc *BasicCluster) SearchPrevRegion(regionKey []byte) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.SearchPrevRegion(regionKey)
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (bc *BasicCluster) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.ScanRange(startKey, endKey, limit)
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (bc *BasicCluster) GetOverlaps(region *RegionInfo) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetOverlaps(region)
}

// Length returns the RegionsInfo length.
func (bc *BasicCluster) Length() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.Length()
}

// RegionSetInformer provides access to a shared informer of regions.
type RegionSetInformer interface {
	RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer))
	GetLeadersWithLock(storeID uint64, callback func(RegionsContainer))
	GetFollowersWithLock(storeID uint64, callback func(RegionsContainer))
	GetAverageRegionSize() int64
	GetStoreRegionCount(storeID uint64) int
	GetRegion(id uint64) *RegionInfo
	ScanRegions(startKey, endKey []byte, limit int) []*RegionInfo
}

// StoreSetInformer provides access to a shared informer of stores.
type StoreSetInformer interface {
	GetStores() []*StoreInfo
	GetStore(id uint64) *StoreInfo

	GetRegionStores(region *RegionInfo) []*StoreInfo
	GetFollowerStores(region *RegionInfo) []*StoreInfo
	GetLeaderStore(region *RegionInfo) *StoreInfo
}

// StoreSetController is used to control stores' status.
type StoreSetController interface {
	BlockStore(id uint64) error
	UnblockStore(id uint64)

	AttachAvailableFunc(id uint64, f func() bool)
}
