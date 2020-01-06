// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"strconv"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
)

const (
	unknown   = "unknown"
	labelType = "label"
)

type storeStatistics struct {
	opt             ScheduleOptions
	Up              int
	Disconnect      int
	Unhealth        int
	Down            int
	Offline         int
	Tombstone       int
	LowSpace        int
	StorageSize     uint64
	StorageCapacity uint64
	RegionCount     int
	LeaderCount     int
	LabelCounter    map[string]int
}

func newStoreStatistics(opt ScheduleOptions) *storeStatistics {
	return &storeStatistics{
		opt:          opt,
		LabelCounter: make(map[string]int),
	}
}

func (s *storeStatistics) Observe(store *core.StoreInfo, stats *StoresStats) {
	for _, k := range s.opt.GetLocationLabels() {
		v := store.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		s.LabelCounter[key]++
	}
	storeAddress := store.GetAddress()
	id := strconv.FormatUint(store.GetID(), 10)
	// Store state.
	switch store.GetState() {
	case metapb.StoreState_Up:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if store.IsUnhealth() {
			s.Unhealth++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else {
			s.Up++
		}
	case metapb.StoreState_Offline:
		s.Offline++
	case metapb.StoreState_Tombstone:
		s.Tombstone++
		s.resetStoreStatistics(storeAddress, id)
		return
	}
	if store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.GetCapacity()
	s.RegionCount += store.GetRegionCount()
	s.LeaderCount += store.GetLeaderCount()

	storeStatusGauge.WithLabelValues(storeAddress, id, "region_score").Set(store.RegionScore(s.opt.GetHighSpaceRatio(), s.opt.GetLowSpaceRatio(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_score").Set(store.LeaderScore(s.opt.GetLeaderScheduleStrategy(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_size").Set(float64(store.GetRegionSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_count").Set(float64(store.GetRegionCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_size").Set(float64(store.GetLeaderSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_count").Set(float64(store.GetLeaderCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_available").Set(float64(store.GetAvailable()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_used").Set(float64(store.GetUsedSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_capacity").Set(float64(store.GetCapacity()))

	// Store flows.
	storeFlowStats := stats.GetRollingStoreStats(store.GetID())
	storeWriteRateBytes, storeReadRateBytes := storeFlowStats.GetBytesRate()
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_bytes").Set(storeWriteRateBytes)
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_bytes").Set(storeReadRateBytes)
	storeWriteRateKeys, storeReadRateKeys := storeFlowStats.GetKeysWriteRate(), storeFlowStats.GetKeysReadRate()
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_keys").Set(storeWriteRateKeys)
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_keys").Set(storeReadRateKeys)
}

func (s *storeStatistics) Collect() {
	metrics := make(map[string]float64)
	metrics["store_up_count"] = float64(s.Up)
	metrics["store_disconnected_count"] = float64(s.Disconnect)
	metrics["store_down_count"] = float64(s.Down)
	metrics["store_unhealth_count"] = float64(s.Unhealth)
	metrics["store_offline_count"] = float64(s.Offline)
	metrics["store_tombstone_count"] = float64(s.Tombstone)
	metrics["store_low_space_count"] = float64(s.LowSpace)
	metrics["region_count"] = float64(s.RegionCount)
	metrics["leader_count"] = float64(s.LeaderCount)
	metrics["storage_size"] = float64(s.StorageSize)
	metrics["storage_capacity"] = float64(s.StorageCapacity)

	for typ, value := range metrics {
		clusterStatusGauge.WithLabelValues(typ).Set(value)
	}

	// Current scheduling configurations of the cluster
	configs := make(map[string]float64)
	configs["leader-schedule-limit"] = float64(s.opt.GetLeaderScheduleLimit())
	configs["region-schedule-limit"] = float64(s.opt.GetRegionScheduleLimit())
	configs["merge-schedule-limit"] = float64(s.opt.GetMergeScheduleLimit())
	configs["replica-schedule-limit"] = float64(s.opt.GetReplicaScheduleLimit())
	configs["max-replicas"] = float64(s.opt.GetMaxReplicas())
	configs["high-space-ratio"] = float64(s.opt.GetHighSpaceRatio())
	configs["low-space-ratio"] = float64(s.opt.GetLowSpaceRatio())
	configs["tolerant-size-ratio"] = float64(s.opt.GetTolerantSizeRatio())
	configs["store-balance-rate"] = float64(s.opt.GetStoreBalanceRate())
	configs["hot-region-schedule-limit"] = float64(s.opt.GetHotRegionScheduleLimit())
	configs["hot-region-cache-hits-threshold"] = float64(s.opt.GetHotRegionCacheHitsThreshold())
	configs["max-pending-peer-count"] = float64(s.opt.GetMaxPendingPeerCount())
	configs["max-snapshot-count"] = float64(s.opt.GetMaxSnapshotCount())
	configs["max-merge-region-size"] = float64(s.opt.GetMaxMergeRegionSize())
	configs["max-merge-region-keys"] = float64(s.opt.GetMaxMergeRegionKeys())

	var enableMakeUpReplica, enableRemoveDownReplica, enableRemoveExtraReplica, enableReplaceOfflineReplica float64
	if s.opt.IsMakeUpReplicaEnabled() {
		enableMakeUpReplica = 1
	}
	if s.opt.IsRemoveDownReplicaEnabled() {
		enableRemoveDownReplica = 1
	}
	if s.opt.IsRemoveExtraReplicaEnabled() {
		enableRemoveExtraReplica = 1
	}
	if s.opt.IsReplaceOfflineReplicaEnabled() {
		enableReplaceOfflineReplica = 1
	}

	configs["enable-makeup-replica"] = enableMakeUpReplica
	configs["enable-remove-down-replica"] = enableRemoveDownReplica
	configs["enable-remove-extra-replica"] = enableRemoveExtraReplica
	configs["enable-replace-offline-replica"] = enableReplaceOfflineReplica

	for typ, value := range configs {
		configStatusGauge.WithLabelValues(typ).Set(value)
	}

	for name, value := range s.LabelCounter {
		placementStatusGauge.WithLabelValues(labelType, name).Set(float64(value))
	}
}

func (s *storeStatistics) resetStoreStatistics(storeAddress string, id string) {
	metrics := []string{
		"region_score",
		"leader_score",
		"region_size",
		"region_count",
		"leader_size",
		"leader_count",
		"store_available",
		"store_used",
		"store_capacity",
		"store_write_rate_bytes",
		"store_read_rate_bytes",
		"store_write_rate_keys",
		"store_read_rate_keys",
	}
	for _, m := range metrics {
		storeStatusGauge.DeleteLabelValues(storeAddress, id, m)
	}
}

type storeStatisticsMap struct {
	opt   ScheduleOptions
	stats *storeStatistics
}

// NewStoreStatisticsMap creates a new storeStatisticsMap.
func NewStoreStatisticsMap(opt ScheduleOptions) *storeStatisticsMap {
	return &storeStatisticsMap{
		opt:   opt,
		stats: newStoreStatistics(opt),
	}
}

func (m *storeStatisticsMap) Observe(store *core.StoreInfo, stats *StoresStats) {
	m.stats.Observe(store, stats)
}

func (m *storeStatisticsMap) Collect() {
	m.stats.Collect()
}

func (m *storeStatisticsMap) Reset() {
	storeStatusGauge.Reset()
	clusterStatusGauge.Reset()
}
