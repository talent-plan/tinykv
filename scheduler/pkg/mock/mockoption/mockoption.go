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
)

const (
	defaultMaxReplicas          = 3
	defaultMaxSnapshotCount     = 3
	defaultMaxPendingPeerCount  = 16
	defaultMaxMergeRegionSize   = 0
	defaultMaxMergeRegionKeys   = 0
	defaultMaxStoreDownTime     = 30 * time.Minute
	defaultLeaderScheduleLimit  = 4
	defaultRegionScheduleLimit  = 64
	defaultReplicaScheduleLimit = 64
)

// ScheduleOptions is a mock of ScheduleOptions
// which implements Options interface
type ScheduleOptions struct {
	RegionScheduleLimit  uint64
	LeaderScheduleLimit  uint64
	ReplicaScheduleLimit uint64
	MaxSnapshotCount     uint64
	MaxPendingPeerCount  uint64
	MaxMergeRegionSize   uint64
	MaxMergeRegionKeys   uint64
	MaxStoreDownTime     time.Duration
	MaxReplicas          int
}

// NewScheduleOptions creates a mock schedule option.
func NewScheduleOptions() *ScheduleOptions {
	mso := &ScheduleOptions{}
	mso.RegionScheduleLimit = defaultRegionScheduleLimit
	mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
	mso.ReplicaScheduleLimit = defaultReplicaScheduleLimit
	mso.MaxSnapshotCount = defaultMaxSnapshotCount
	mso.MaxMergeRegionSize = defaultMaxMergeRegionSize
	mso.MaxMergeRegionKeys = defaultMaxMergeRegionKeys
	mso.MaxStoreDownTime = defaultMaxStoreDownTime
	mso.MaxReplicas = defaultMaxReplicas
	mso.MaxPendingPeerCount = defaultMaxPendingPeerCount
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

// GetMaxMergeRegionSize mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionSize() uint64 {
	return mso.MaxMergeRegionSize
}

// GetMaxMergeRegionKeys mocks method
func (mso *ScheduleOptions) GetMaxMergeRegionKeys() uint64 {
	return mso.MaxMergeRegionKeys
}

// GetMaxStoreDownTime mocks method
func (mso *ScheduleOptions) GetMaxStoreDownTime() time.Duration {
	return mso.MaxStoreDownTime
}

// GetMaxReplicas mocks method
func (mso *ScheduleOptions) GetMaxReplicas() int {
	return mso.MaxReplicas
}

// SetMaxReplicas mocks method
func (mso *ScheduleOptions) SetMaxReplicas(replicas int) {
	mso.MaxReplicas = replicas
}
