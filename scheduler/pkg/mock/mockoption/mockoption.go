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

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
)

const (
	defaultMaxReplicas                 = 3
	defaultMaxSnapshotCount            = 3
	defaultMaxPendingPeerCount         = 16
	defaultMaxMergeRegionSize          = 0
	defaultMaxMergeRegionKeys          = 0
	defaultMaxStoreDownTime            = 30 * time.Minute
	defaultLeaderScheduleLimit         = 4
	defaultRegionScheduleLimit         = 64
	defaultReplicaScheduleLimit        = 64
	defaultMergeScheduleLimit          = 8
	defaultStoreBalanceRate            = 60
	defaultTolerantSizeRatio           = 2.5
	defaultSchedulerMaxWaitingOperator = 3
	defaultLeaderScheduleStrategy      = "count"
	defaultKeyType                     = "table"
)

// ScheduleOptions is a mock of ScheduleOptions
// which implements Options interface
type ScheduleOptions struct {
	RegionScheduleLimit          uint64
	LeaderScheduleLimit          uint64
	ReplicaScheduleLimit         uint64
	MergeScheduleLimit           uint64
	StoreBalanceRate             float64
	MaxSnapshotCount             uint64
	MaxPendingPeerCount          uint64
	MaxMergeRegionSize           uint64
	MaxMergeRegionKeys           uint64
	SchedulerMaxWaitingOperator  uint64
	EnableOneWayMerge            bool
	EnableCrossTableMerge        bool
	KeyType                      string
	MaxStoreDownTime             time.Duration
	MaxReplicas                  int
	TolerantSizeRatio            float64
	EnableRemoveDownReplica      bool
	EnableReplaceOfflineReplica  bool
	EnableMakeUpReplica          bool
	EnableRemoveExtraReplica     bool
	DisableRemoveDownReplica     bool
	DisableReplaceOfflineReplica bool
	DisableMakeUpReplica         bool
	DisableRemoveExtraReplica    bool
	LeaderScheduleStrategy       string
}

// NewScheduleOptions creates a mock schedule option.
func NewScheduleOptions() *ScheduleOptions {
	mso := &ScheduleOptions{}
	mso.RegionScheduleLimit = defaultRegionScheduleLimit
	mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
	mso.ReplicaScheduleLimit = defaultReplicaScheduleLimit
	mso.MergeScheduleLimit = defaultMergeScheduleLimit
	mso.StoreBalanceRate = defaultStoreBalanceRate
	mso.MaxSnapshotCount = defaultMaxSnapshotCount
	mso.MaxMergeRegionSize = defaultMaxMergeRegionSize
	mso.MaxMergeRegionKeys = defaultMaxMergeRegionKeys
	mso.SchedulerMaxWaitingOperator = defaultSchedulerMaxWaitingOperator
	mso.MaxStoreDownTime = defaultMaxStoreDownTime
	mso.MaxReplicas = defaultMaxReplicas
	mso.MaxPendingPeerCount = defaultMaxPendingPeerCount
	mso.TolerantSizeRatio = defaultTolerantSizeRatio
	mso.EnableRemoveDownReplica = true
	mso.EnableReplaceOfflineReplica = true
	mso.EnableMakeUpReplica = true
	mso.EnableRemoveExtraReplica = true
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

// GetStoreBalanceRate mocks method
func (mso *ScheduleOptions) GetStoreBalanceRate() float64 {
	return mso.StoreBalanceRate
}

// GetMaxMergeRegionSize mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionSize() uint64 {
	return mso.MaxMergeRegionSize
}

// GetMaxMergeRegionKeys mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionKeys() uint64 {
	return mso.MaxMergeRegionKeys
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

// GetTolerantSizeRatio mocks method
func (mso *ScheduleOptions) GetTolerantSizeRatio() float64 {
	return mso.TolerantSizeRatio
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

// GetLeaderScheduleStrategy is to get leader schedule strategy.
func (mso *ScheduleOptions) GetLeaderScheduleStrategy() core.ScheduleStrategy {
	return core.StringToScheduleStrategy(mso.LeaderScheduleStrategy)
}

// GetKeyType is to get key type.
func (mso *ScheduleOptions) GetKeyType() core.KeyType {
	return core.StringToKeyType(mso.KeyType)
}
