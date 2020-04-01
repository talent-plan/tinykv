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

package core

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// StoreInfo contains information about a store.
type StoreInfo struct {
	meta  *metapb.Store
	stats *schedulerpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked          bool
	leaderCount      int
	regionCount      int
	leaderSize       int64
	regionSize       int64
	pendingPeerCount int
	lastHeartbeatTS  time.Time
	leaderWeight     float64
	regionWeight     float64
	available        func() bool
}

// NewStoreInfo creates StoreInfo with meta data.
func NewStoreInfo(store *metapb.Store, opts ...StoreCreateOption) *StoreInfo {
	storeInfo := &StoreInfo{
		meta:         store,
		stats:        &schedulerpb.StoreStats{},
		leaderWeight: 1.0,
		regionWeight: 1.0,
	}
	for _, opt := range opts {
		opt(storeInfo)
	}
	return storeInfo
}

// Clone creates a copy of current StoreInfo.
func (s *StoreInfo) Clone(opts ...StoreCreateOption) *StoreInfo {
	meta := proto.Clone(s.meta).(*metapb.Store)
	store := &StoreInfo{
		meta:             meta,
		stats:            s.stats,
		blocked:          s.blocked,
		leaderCount:      s.leaderCount,
		regionCount:      s.regionCount,
		leaderSize:       s.leaderSize,
		regionSize:       s.regionSize,
		pendingPeerCount: s.pendingPeerCount,
		lastHeartbeatTS:  s.lastHeartbeatTS,
		leaderWeight:     s.leaderWeight,
		regionWeight:     s.regionWeight,
		available:        s.available,
	}

	for _, opt := range opts {
		opt(store)
	}
	return store
}

// IsBlocked returns if the store is blocked.
func (s *StoreInfo) IsBlocked() bool {
	return s.blocked
}

// IsAvailable returns if the store bucket of limitation is available
func (s *StoreInfo) IsAvailable() bool {
	if s.available == nil {
		return true
	}
	return s.available()
}

// IsUp checks if the store's state is Up.
func (s *StoreInfo) IsUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

// IsOffline checks if the store's state is Offline.
func (s *StoreInfo) IsOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

// IsTombstone checks if the store's state is Tombstone.
func (s *StoreInfo) IsTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

// DownTime returns the time elapsed since last heartbeat.
func (s *StoreInfo) DownTime() time.Duration {
	return time.Since(s.GetLastHeartbeatTS())
}

// GetMeta returns the meta information of the store.
func (s *StoreInfo) GetMeta() *metapb.Store {
	return s.meta
}

// GetState returns the state of the store.
func (s *StoreInfo) GetState() metapb.StoreState {
	return s.meta.GetState()
}

// GetAddress returns the address of the store.
func (s *StoreInfo) GetAddress() string {
	return s.meta.GetAddress()
}

// GetID returns the ID of the store.
func (s *StoreInfo) GetID() uint64 {
	return s.meta.GetId()
}

// GetStoreStats returns the statistics information of the store.
func (s *StoreInfo) GetStoreStats() *schedulerpb.StoreStats {
	return s.stats
}

// GetCapacity returns the capacity size of the store.
func (s *StoreInfo) GetCapacity() uint64 {
	return s.stats.GetCapacity()
}

// GetAvailable returns the available size of the store.
func (s *StoreInfo) GetAvailable() uint64 {
	return s.stats.GetAvailable()
}

// GetUsedSize returns the used size of the store.
func (s *StoreInfo) GetUsedSize() uint64 {
	return s.stats.GetUsedSize()
}

// IsBusy returns if the store is busy.
func (s *StoreInfo) IsBusy() bool {
	return s.stats.GetIsBusy()
}

// GetSendingSnapCount returns the current sending snapshot count of the store.
func (s *StoreInfo) GetSendingSnapCount() uint32 {
	return s.stats.GetSendingSnapCount()
}

// GetReceivingSnapCount returns the current receiving snapshot count of the store.
func (s *StoreInfo) GetReceivingSnapCount() uint32 {
	return s.stats.GetReceivingSnapCount()
}

// GetApplyingSnapCount returns the current applying snapshot count of the store.
func (s *StoreInfo) GetApplyingSnapCount() uint32 {
	return s.stats.GetApplyingSnapCount()
}

// GetStartTime returns the start time of the store.
func (s *StoreInfo) GetStartTime() uint32 {
	return s.stats.GetStartTime()
}

// GetLeaderCount returns the leader count of the store.
func (s *StoreInfo) GetLeaderCount() int {
	return s.leaderCount
}

// GetRegionCount returns the Region count of the store.
func (s *StoreInfo) GetRegionCount() int {
	return s.regionCount
}

// GetLeaderSize returns the leader size of the store.
func (s *StoreInfo) GetLeaderSize() int64 {
	return s.leaderSize
}

// GetRegionSize returns the Region size of the store.
func (s *StoreInfo) GetRegionSize() int64 {
	return s.regionSize
}

// GetPendingPeerCount returns the pending peer count of the store.
func (s *StoreInfo) GetPendingPeerCount() int {
	return s.pendingPeerCount
}

// GetLeaderWeight returns the leader weight of the store.
func (s *StoreInfo) GetLeaderWeight() float64 {
	return s.leaderWeight
}

// GetRegionWeight returns the Region weight of the store.
func (s *StoreInfo) GetRegionWeight() float64 {
	return s.regionWeight
}

// GetLastHeartbeatTS returns the last heartbeat timestamp of the store.
func (s *StoreInfo) GetLastHeartbeatTS() time.Time {
	return s.lastHeartbeatTS
}

const minWeight = 1e-6

// StorageSize returns store's used storage size reported from tikv.
func (s *StoreInfo) StorageSize() uint64 {
	return s.GetUsedSize()
}

// AvailableRatio is store's freeSpace/capacity.
func (s *StoreInfo) AvailableRatio() float64 {
	if s.GetCapacity() == 0 {
		return 0
	}
	return float64(s.GetAvailable()) / float64(s.GetCapacity())
}

// IsLowSpace checks if the store is lack of space.
func (s *StoreInfo) IsLowSpace(lowSpaceRatio float64) bool {
	return s.GetStoreStats() != nil && s.AvailableRatio() < 1-lowSpaceRatio
}

// ResourceCount returns count of leader/region in the store.
func (s *StoreInfo) ResourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return uint64(s.GetLeaderCount())
	case RegionKind:
		return uint64(s.GetRegionCount())
	default:
		return 0
	}
}

// ResourceSize returns size of leader/region in the store
func (s *StoreInfo) ResourceSize(kind ResourceKind) int64 {
	switch kind {
	case LeaderKind:
		return s.GetLeaderSize()
	case RegionKind:
		return s.GetRegionSize()
	default:
		return 0
	}
}

// ResourceWeight returns weight of leader/region in the score
func (s *StoreInfo) ResourceWeight(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		leaderWeight := s.GetLeaderWeight()
		if leaderWeight <= 0 {
			return minWeight
		}
		return leaderWeight
	case RegionKind:
		regionWeight := s.GetRegionWeight()
		if regionWeight <= 0 {
			return minWeight
		}
		return regionWeight
	default:
		return 0
	}
}

// GetStartTS returns the start timestamp.
func (s *StoreInfo) GetStartTS() time.Time {
	return time.Unix(int64(s.GetStartTime()), 0)
}

// GetUptime returns the uptime.
func (s *StoreInfo) GetUptime() time.Duration {
	uptime := s.GetLastHeartbeatTS().Sub(s.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

var (
	// If a store's last heartbeat is storeDisconnectDuration ago, the store will
	// be marked as disconnected state. The value should be greater than tikv's
	// store heartbeat interval (default 10s).
	storeDisconnectDuration = 20 * time.Second
	storeUnhealthDuration   = 10 * time.Minute
)

// IsDisconnected checks if a store is disconnected, which means PD misses
// tikv's store heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (s *StoreInfo) IsDisconnected() bool {
	return s.DownTime() > storeDisconnectDuration
}

// IsUnhealth checks if a store is unhealth.
func (s *StoreInfo) IsUnhealth() bool {
	return s.DownTime() > storeUnhealthDuration
}

type storeNotFoundErr struct {
	storeID uint64
}

func (e storeNotFoundErr) Error() string {
	return fmt.Sprintf("store %v not found", e.storeID)
}

// NewStoreNotFoundErr is for log of store not found
func NewStoreNotFoundErr(storeID uint64) errcode.ErrorCode {
	return errcode.NewNotFoundErr(storeNotFoundErr{storeID})
}

// StoresInfo contains information about all stores.
type StoresInfo struct {
	stores map[uint64]*StoreInfo
}

// NewStoresInfo create a StoresInfo with map of storeID to StoreInfo
func NewStoresInfo() *StoresInfo {
	return &StoresInfo{
		stores: make(map[uint64]*StoreInfo),
	}
}

// GetStore returns a copy of the StoreInfo with the specified storeID.
func (s *StoresInfo) GetStore(storeID uint64) *StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// TakeStore returns the point of the origin StoreInfo with the specified storeID.
func (s *StoresInfo) TakeStore(storeID uint64) *StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// SetStore sets a StoreInfo with storeID.
func (s *StoresInfo) SetStore(store *StoreInfo) {
	s.stores[store.GetID()] = store
}

// BlockStore blocks a StoreInfo with storeID.
func (s *StoresInfo) BlockStore(storeID uint64) errcode.ErrorCode {
	op := errcode.Op("store.block")
	store, ok := s.stores[storeID]
	if !ok {
		return op.AddTo(NewStoreNotFoundErr(storeID))
	}
	if store.IsBlocked() {
		return op.AddTo(StoreBlockedErr{StoreID: storeID})
	}
	s.stores[storeID] = store.Clone(SetStoreBlock())
	return nil
}

// UnblockStore unblocks a StoreInfo with storeID.
func (s *StoresInfo) UnblockStore(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		log.Fatal("store is unblocked, but it is not found",
			zap.Uint64("store-id", storeID))
	}
	s.stores[storeID] = store.Clone(SetStoreUnBlock())
}

// AttachAvailableFunc attaches f to a specific store.
func (s *StoresInfo) AttachAvailableFunc(storeID uint64, f func() bool) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetAvailableFunc(f))
	}
}

// GetStores gets a complete set of StoreInfo.
func (s *StoresInfo) GetStores() []*StoreInfo {
	stores := make([]*StoreInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	return stores
}

// GetMetaStores gets a complete set of metapb.Store.
func (s *StoresInfo) GetMetaStores() []*metapb.Store {
	stores := make([]*metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.GetMeta())
	}
	return stores
}

// DeleteStore deletes tombstone record form store
func (s *StoresInfo) DeleteStore(store *StoreInfo) {
	delete(s.stores, store.GetID())
}

// GetStoreCount returns the total count of storeInfo.
func (s *StoresInfo) GetStoreCount() int {
	return len(s.stores)
}

// SetLeaderCount sets the leader count to a storeInfo.
func (s *StoresInfo) SetLeaderCount(storeID uint64, leaderCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetLeaderCount(leaderCount))
	}
}

// SetRegionCount sets the region count to a storeInfo.
func (s *StoresInfo) SetRegionCount(storeID uint64, regionCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetRegionCount(regionCount))
	}
}

// SetPendingPeerCount sets the pending count to a storeInfo.
func (s *StoresInfo) SetPendingPeerCount(storeID uint64, pendingPeerCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetPendingPeerCount(pendingPeerCount))
	}
}

// SetLeaderSize sets the leader size to a storeInfo.
func (s *StoresInfo) SetLeaderSize(storeID uint64, leaderSize int64) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetLeaderSize(leaderSize))
	}
}

// SetRegionSize sets the region size to a storeInfo.
func (s *StoresInfo) SetRegionSize(storeID uint64, regionSize int64) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetRegionSize(regionSize))
	}
}

// UpdateStoreStatus updates the information of the store.
func (s *StoresInfo) UpdateStoreStatus(storeID uint64, leaderCount int, regionCount int, pendingPeerCount int, leaderSize int64, regionSize int64) {
	if store, ok := s.stores[storeID]; ok {
		newStore := store.Clone(SetLeaderCount(leaderCount),
			SetRegionCount(regionCount),
			SetPendingPeerCount(pendingPeerCount),
			SetLeaderSize(leaderSize),
			SetRegionSize(regionSize))
		s.SetStore(newStore)
	}
}
