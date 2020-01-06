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

import "time"

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int

	Kind      FlowKind `json:"kind"`
	BytesRate float64  `json:"flow_bytes"`
	KeysRate  float64  `json:"flow_keys"`
	// RollingBytesRate is a rolling statistics, recording some recently added records.
	RollingBytesRate MovingAvg

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// Version used to check the region split times
	Version uint64

	needDelete bool
	isLeader   bool
	isNew      bool
}

// IsNeedDelete to delete the item in cache.
func (stat *HotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicaes the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicaes the item is first update in the cache of the region.
func (stat *HotPeerStat) IsNew() bool {
	return stat.isNew
}

// GetBytesRate returns denoised BytesRate if possible.
func (stat *HotPeerStat) GetBytesRate() float64 {
	if stat.RollingBytesRate == nil {
		return stat.BytesRate
	}
	return stat.RollingBytesRate.Get()
}
