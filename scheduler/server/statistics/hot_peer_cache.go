// Copyright 2019 PingCAP, Inc.
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

package statistics

import (
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/cache"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
)

const (
	cacheMaxLen     = 1000
	hotPeerMaxCount = 400

	rollingWindowsSize = 5

	hotWriteRegionMinBytesRate = 16 * 1024
	hotReadRegionMinBytesRate  = 128 * 1024

	hotRegionReportMinInterval = 3

	hotRegionAntiCount = 1
)

// hotPeerCache saves the hotspot peer's statistics.
type hotPeerCache struct {
	kind           FlowKind
	peersOfStore   map[uint64]cache.Cache         // storeID -> hot peers
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	return &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]cache.Cache),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = cache.NewCache(cacheMaxLen, cache.TwoQueueCache)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item.RegionID, item)

		stores, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo, stats *StoresStats) (ret []*HotPeerStat) {
	storeIDs := f.getAllStoreIDs(region)

	totalBytes := float64(f.getTotalBytes(region))
	totalKeys := float64(f.getTotalKeys(region))

	bytesPerSecInit := totalBytes / RegionHeartBeatReportInterval
	keysPerSecInit := totalKeys / RegionHeartBeatReportInterval

	for storeID := range storeIDs {
		bytesPerSec := bytesPerSecInit
		keysPerSec := keysPerSecInit
		isExpired := f.isRegionExpired(region, storeID)
		oldItem := f.getOldHotPeerStat(region.GetID(), storeID)

		// This is used for the simulator.
		if oldItem != nil && Denoising {
			interval := time.Since(oldItem.LastUpdateTime).Seconds()
			// ignore if report too fast
			if interval < hotRegionReportMinInterval && !isExpired {
				continue
			}
			bytesPerSec = totalBytes / interval
			keysPerSec = totalKeys / interval
		}

		newItem := &HotPeerStat{
			StoreID:        storeID,
			RegionID:       region.GetID(),
			Kind:           f.kind,
			BytesRate:      bytesPerSec,
			KeysRate:       keysPerSec,
			LastUpdateTime: time.Now(),
			Version:        region.GetMeta().GetRegionEpoch().GetVersion(),
			needDelete:     isExpired,
			isLeader:       region.GetLeader().GetStoreId() == storeID,
		}

		hotThreshold := f.calcHotThreshold(stats, storeID)
		newItem = updateHotPeerStat(newItem, oldItem, bytesPerSec, hotThreshold)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithPeer(region, region.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(stats *StoresStats, typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		threshold := f.calcHotThreshold(stats, storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(threshold)
	}
}

func (f *hotPeerCache) getTotalBytes(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetBytesWritten()
	case ReadFlow:
		return region.GetBytesRead()
	}
	return 0
}

func (f *hotPeerCache) getTotalKeys(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetKeysWritten()
	case ReadFlow:
		return region.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v, ok := hotPeers.Peek(regionID); ok {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isRegionExpired(region *core.RegionInfo, storeID uint64) bool {
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

func (f *hotPeerCache) calcHotThreshold(stats *StoresStats, storeID uint64) float64 {
	switch f.kind {
	case WriteFlow:
		return calculateWriteHotThresholdWithStore(stats, storeID)
	case ReadFlow:
		return calculateReadHotThresholdWithStore(stats, storeID)
	}
	return 0
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) map[uint64]struct{} {
	storeIDs := make(map[uint64]struct{})
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
		}
	}

	return storeIDs
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat, ok := peers.Peek(region.GetID()); ok {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func updateHotPeerStat(newItem, oldItem *HotPeerStat, bytesRate float64, hotThreshold float64) *HotPeerStat {
	isHot := bytesRate >= hotThreshold
	if newItem.needDelete {
		return newItem
	}
	if oldItem != nil {
		newItem.RollingBytesRate = oldItem.RollingBytesRate
		if isHot {
			newItem.HotDegree = oldItem.HotDegree + 1
			newItem.AntiCount = hotRegionAntiCount
		} else {
			newItem.HotDegree = oldItem.HotDegree - 1
			newItem.AntiCount = oldItem.AntiCount - 1
			if newItem.AntiCount < 0 {
				newItem.needDelete = true
			}
		}
	} else {
		if !isHot {
			return nil
		}
		newItem.RollingBytesRate = NewMedianFilter(rollingWindowsSize)
		newItem.AntiCount = hotRegionAntiCount
		newItem.isNew = true
	}
	newItem.RollingBytesRate.Add(bytesRate)

	return newItem
}

// Utils
func calculateWriteHotThresholdWithStore(stats *StoresStats, storeID uint64) float64 {
	writeBytes, _ := stats.GetStoreBytesRate(storeID)
	hotRegionThreshold := writeBytes / hotPeerMaxCount

	if hotRegionThreshold < hotWriteRegionMinBytesRate {
		hotRegionThreshold = hotWriteRegionMinBytesRate
	}
	return hotRegionThreshold
}

func calculateReadHotThresholdWithStore(stats *StoresStats, storeID uint64) float64 {
	_, readBytes := stats.GetStoreBytesRate(storeID)
	hotRegionThreshold := readBytes / hotPeerMaxCount

	if hotRegionThreshold < hotReadRegionMinBytesRate {
		hotRegionThreshold = hotReadRegionMinBytesRate
	}
	return hotRegionThreshold
}
