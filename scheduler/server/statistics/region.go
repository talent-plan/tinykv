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
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
)

const (
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region.
	RegionHeartBeatReportInterval = 60
)

// RegionStats records a list of regions' statistics and distribution status.
type RegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StorageKeys      int64            `json:"storage_keys"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StoreLeaderKeys  map[uint64]int64 `json:"store_leader_keys"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
	StorePeerKeys    map[uint64]int64 `json:"store_peer_keys"`
}

// GetRegionStats sums regions' statistics.
func GetRegionStats(regions []*core.RegionInfo) *RegionStats {
	stats := newRegionStats()
	for _, region := range regions {
		stats.Observe(region)
	}
	return stats
}

func newRegionStats() *RegionStats {
	return &RegionStats{
		StoreLeaderCount: make(map[uint64]int),
		StorePeerCount:   make(map[uint64]int),
		StoreLeaderSize:  make(map[uint64]int64),
		StoreLeaderKeys:  make(map[uint64]int64),
		StorePeerSize:    make(map[uint64]int64),
		StorePeerKeys:    make(map[uint64]int64),
	}
}

// Observe adds a region's statistics into RegionStats.
func (s *RegionStats) Observe(r *core.RegionInfo) {
	s.Count++
	approximateKeys := r.GetApproximateKeys()
	approximateSize := r.GetApproximateSize()
	if approximateSize <= core.EmptyRegionApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += approximateSize
	s.StorageKeys += approximateKeys
	leader := r.GetLeader()
	if leader != nil {
		storeID := leader.GetStoreId()
		s.StoreLeaderCount[storeID]++
		s.StoreLeaderSize[storeID] += approximateSize
		s.StoreLeaderKeys[storeID] += approximateKeys
	}
	peers := r.GetMeta().GetPeers()
	for _, p := range peers {
		storeID := p.GetStoreId()
		s.StorePeerCount[storeID]++
		s.StorePeerSize[storeID] += approximateSize
		s.StorePeerKeys[storeID] += approximateKeys
	}
}
