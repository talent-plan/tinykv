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

package mockoption

import (
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
)

const (
	defaultMaxReplicas                 = 3
	defaultMaxSnapshotCount            = 3
	defaultMaxPendingPeerCount         = 16
	defaultMaxMergeRegionSize          = 0
	defaultMaxMergeRegionKeys          = 0
	defaultSplitMergeInterval          = 0
	defaultMaxStoreDownTime            = 30 * time.Minute
	defaultLeaderScheduleLimit         = 4
	defaultRegionScheduleLimit         = 64
	defaultReplicaScheduleLimit        = 64
	defaultMergeScheduleLimit          = 8
	defaultHotRegionScheduleLimit      = 4
	defaultStoreBalanceRate            = 60
	defaultTolerantSizeRatio           = 2.5
	defaultLowSpaceRatio               = 0.8
	defaultHighSpaceRatio              = 0.6
	defaultSchedulerMaxWaitingOperator = 3
	defaultHotRegionCacheHitsThreshold = 3
	defaultStrictlyMatchLabel          = true
	defaultLeaderScheduleStrategy      = "count"
	defaultEnablePlacementRules        = false
	defaultKeyType                     = "table"
)

// ScheduleOptions is a mock of ScheduleOptions
// which implements Options interface
type ScheduleOptions struct {
	RegionScheduleLimit          uint64
	LeaderScheduleLimit          uint64
	ReplicaScheduleLimit         uint64
	MergeScheduleLimit           uint64
	HotRegionScheduleLimit       uint64
	StoreBalanceRate             float64
	MaxSnapshotCount             uint64
	MaxPendingPeerCount          uint64
	MaxMergeRegionSize           uint64
	MaxMergeRegionKeys           uint64
	SchedulerMaxWaitingOperator  uint64
	SplitMergeInterval           time.Duration
	EnableOneWayMerge            bool
	EnableCrossTableMerge        bool
	KeyType                      string
	MaxStoreDownTime             time.Duration
	MaxReplicas                  int
	LocationLabels               []string
	StrictlyMatchLabel           bool
	HotRegionCacheHitsThreshold  int
	TolerantSizeRatio            float64
	LowSpaceRatio                float64
	HighSpaceRatio               float64
	EnableRemoveDownReplica      bool
	EnableReplaceOfflineReplica  bool
	EnableMakeUpReplica          bool
	EnableRemoveExtraReplica     bool
	EnableLocationReplacement    bool
	EnablePlacementRules         bool
	DisableRemoveDownReplica     bool
	DisableReplaceOfflineReplica bool
	DisableMakeUpReplica         bool
	DisableRemoveExtraReplica    bool
	DisableLocationReplacement   bool
	LeaderScheduleStrategy       string
	LabelProperties              map[string][]*metapb.StoreLabel
}

// NewScheduleOptions creates a mock schedule option.
func NewScheduleOptions() *ScheduleOptions {
	mso := &ScheduleOptions{}
	mso.RegionScheduleLimit = defaultRegionScheduleLimit
	mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
	mso.ReplicaScheduleLimit = defaultReplicaScheduleLimit
	mso.MergeScheduleLimit = defaultMergeScheduleLimit
	mso.HotRegionScheduleLimit = defaultHotRegionScheduleLimit
	mso.StoreBalanceRate = defaultStoreBalanceRate
	mso.MaxSnapshotCount = defaultMaxSnapshotCount
	mso.MaxMergeRegionSize = defaultMaxMergeRegionSize
	mso.MaxMergeRegionKeys = defaultMaxMergeRegionKeys
	mso.SchedulerMaxWaitingOperator = defaultSchedulerMaxWaitingOperator
	mso.SplitMergeInterval = defaultSplitMergeInterval
	mso.MaxStoreDownTime = defaultMaxStoreDownTime
	mso.MaxReplicas = defaultMaxReplicas
	mso.StrictlyMatchLabel = defaultStrictlyMatchLabel
	mso.EnablePlacementRules = defaultEnablePlacementRules
	mso.HotRegionCacheHitsThreshold = defaultHotRegionCacheHitsThreshold
	mso.MaxPendingPeerCount = defaultMaxPendingPeerCount
	mso.TolerantSizeRatio = defaultTolerantSizeRatio
	mso.LowSpaceRatio = defaultLowSpaceRatio
	mso.HighSpaceRatio = defaultHighSpaceRatio
	mso.EnableRemoveDownReplica = true
	mso.EnableReplaceOfflineReplica = true
	mso.EnableMakeUpReplica = true
	mso.EnableRemoveExtraReplica = true
	mso.EnableLocationReplacement = true
	mso.LeaderScheduleStrategy = defaultLeaderScheduleStrategy
	mso.KeyType = defaultKeyType
	return mso
}

// GetLeaderScheduleLimit mocks method
func (mso *ScheduleOptions) GetLeaderScheduleLimit() uint64 {
	return mso.LeaderScheduleLimit
}

// GetRegionScheduleLimit mocks method
func (mso *ScheduleOptions) GetRegionScheduleLimit() uint64 {
	return mso.RegionScheduleLimit
}

// GetReplicaScheduleLimit mocks method
func (mso *ScheduleOptions) GetReplicaScheduleLimit() uint64 {
	return mso.ReplicaScheduleLimit
}

// GetMergeScheduleLimit mocks method
func (mso *ScheduleOptions) GetMergeScheduleLimit() uint64 {
	return mso.MergeScheduleLimit
}

// GetHotRegionScheduleLimit mocks method
func (mso *ScheduleOptions) GetHotRegionScheduleLimit() uint64 {
	return mso.HotRegionScheduleLimit
}

// GetStoreBalanceRate mocks method
func (mso *ScheduleOptions) GetStoreBalanceRate() float64 {
	return mso.StoreBalanceRate
}

// GetMaxSnapshotCount mocks method
func (mso *ScheduleOptions) GetMaxSnapshotCount() uint64 {
	return mso.MaxSnapshotCount
}

// GetMaxPendingPeerCount mocks method
func (mso *ScheduleOptions) GetMaxPendingPeerCount() uint64 {
	return mso.MaxPendingPeerCount
}

// GetMaxMergeRegionSize mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionSize() uint64 {
	return mso.MaxMergeRegionSize
}

// GetMaxMergeRegionKeys mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionKeys() uint64 {
	return mso.MaxMergeRegionKeys
}

// GetSplitMergeInterval mocks method
func (mso *ScheduleOptions) GetSplitMergeInterval() time.Duration {
	return mso.SplitMergeInterval
}

// IsOneWayMergeEnabled mocks method
func (mso *ScheduleOptions) IsOneWayMergeEnabled() bool {
	return mso.EnableOneWayMerge
}

// IsCrossTableMergeEnabled mocks method
func (mso *ScheduleOptions) IsCrossTableMergeEnabled() bool {
	return mso.EnableCrossTableMerge
}

// GetMaxStoreDownTime mocks method
func (mso *ScheduleOptions) GetMaxStoreDownTime() time.Duration {
	return mso.MaxStoreDownTime
}

// GetMaxReplicas mocks method
func (mso *ScheduleOptions) GetMaxReplicas() int {
	return mso.MaxReplicas
}

// GetLocationLabels mocks method
func (mso *ScheduleOptions) GetLocationLabels() []string {
	return mso.LocationLabels
}

// GetStrictlyMatchLabel mocks method
func (mso *ScheduleOptions) GetStrictlyMatchLabel() bool {
	return mso.StrictlyMatchLabel
}

// IsPlacementRulesEnabled mocks method
func (mso *ScheduleOptions) IsPlacementRulesEnabled() bool {
	return mso.EnablePlacementRules
}

// GetHotRegionCacheHitsThreshold mocks method
func (mso *ScheduleOptions) GetHotRegionCacheHitsThreshold() int {
	return mso.HotRegionCacheHitsThreshold
}

// GetTolerantSizeRatio mocks method
func (mso *ScheduleOptions) GetTolerantSizeRatio() float64 {
	return mso.TolerantSizeRatio
}

// GetLowSpaceRatio mocks method
func (mso *ScheduleOptions) GetLowSpaceRatio() float64 {
	return mso.LowSpaceRatio
}

// GetHighSpaceRatio mocks method
func (mso *ScheduleOptions) GetHighSpaceRatio() float64 {
	return mso.HighSpaceRatio
}

// GetSchedulerMaxWaitingOperator mocks method.
func (mso *ScheduleOptions) GetSchedulerMaxWaitingOperator() uint64 {
	return mso.SchedulerMaxWaitingOperator
}

// SetMaxReplicas mocks method
func (mso *ScheduleOptions) SetMaxReplicas(replicas int) {
	mso.MaxReplicas = replicas
}

// IsRemoveDownReplicaEnabled mocks method.
func (mso *ScheduleOptions) IsRemoveDownReplicaEnabled() bool {
	return mso.EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled mocks method.
func (mso *ScheduleOptions) IsReplaceOfflineReplicaEnabled() bool {
	return mso.EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled mocks method.
func (mso *ScheduleOptions) IsMakeUpReplicaEnabled() bool {
	return mso.EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled mocks method.
func (mso *ScheduleOptions) IsRemoveExtraReplicaEnabled() bool {
	return mso.EnableRemoveExtraReplica
}

// IsLocationReplacementEnabled mocks method.
func (mso *ScheduleOptions) IsLocationReplacementEnabled() bool {
	return mso.EnableLocationReplacement
}

// GetLeaderScheduleStrategy is to get leader schedule strategy.
func (mso *ScheduleOptions) GetLeaderScheduleStrategy() core.ScheduleStrategy {
	return core.StringToScheduleStrategy(mso.LeaderScheduleStrategy)
}

// GetKeyType is to get key type.
func (mso *ScheduleOptions) GetKeyType() core.KeyType {
	return core.StringToKeyType(mso.KeyType)
}
