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

package config

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
)

// ScheduleOption is a wrapper to access the configuration safely.
type ScheduleOption struct {
	schedule       atomic.Value
	replication    *Replication
	labelProperty  atomic.Value
	clusterVersion unsafe.Pointer
	pdServerConfig atomic.Value
}

// NewScheduleOption creates a new ScheduleOption.
func NewScheduleOption(cfg *Config) *ScheduleOption {
	o := &ScheduleOption{}
	o.Store(&cfg.Schedule)
	o.replication = newReplication(&cfg.Replication)
	o.pdServerConfig.Store(&cfg.PDServerCfg)
	o.labelProperty.Store(cfg.LabelProperty)
	o.SetClusterVersion(&cfg.ClusterVersion)
	return o
}

// Load returns scheduling configurations.
func (o *ScheduleOption) Load() *ScheduleConfig {
	return o.schedule.Load().(*ScheduleConfig)
}

// Store sets scheduling configurations.
func (o *ScheduleOption) Store(cfg *ScheduleConfig) {
	o.schedule.Store(cfg)
}

// GetReplication returns replication configurations.
func (o *ScheduleOption) GetReplication() *Replication {
	return o.replication
}

// SetPDServerConfig sets the PD configuration.
func (o *ScheduleOption) SetPDServerConfig(cfg *PDServerConfig) {
	o.pdServerConfig.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (o *ScheduleOption) GetMaxReplicas() int {
	return o.replication.GetMaxReplicas()
}

// SetMaxReplicas sets the number of replicas for each region.
func (o *ScheduleOption) SetMaxReplicas(replicas int) {
	o.replication.SetMaxReplicas(replicas)
}

// GetLocationLabels returns the location labels for each region.
func (o *ScheduleOption) GetLocationLabels() []string {
	return o.replication.GetLocationLabels()
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *ScheduleOption) IsPlacementRulesEnabled() bool {
	return o.replication.IsPlacementRulesEnabled()
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *ScheduleOption) GetMaxSnapshotCount() uint64 {
	return o.Load().MaxSnapshotCount
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *ScheduleOption) GetMaxPendingPeerCount() uint64 {
	return o.Load().MaxPendingPeerCount
}

// GetMaxMergeRegionSize returns the max region size.
func (o *ScheduleOption) GetMaxMergeRegionSize() uint64 {
	return o.Load().MaxMergeRegionSize
}

// GetMaxMergeRegionKeys returns the max number of keys.
func (o *ScheduleOption) GetMaxMergeRegionKeys() uint64 {
	return o.Load().MaxMergeRegionKeys
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *ScheduleOption) GetSplitMergeInterval() time.Duration {
	return o.Load().SplitMergeInterval.Duration
}

// SetSplitMergeInterval to set the interval between finishing split and starting to merge. It's only used to test.
func (o *ScheduleOption) SetSplitMergeInterval(splitMergeInterval time.Duration) {
	o.Load().SplitMergeInterval = typeutil.Duration{Duration: splitMergeInterval}
}

// IsOneWayMergeEnabled returns if a region can only be merged into the next region of it.
func (o *ScheduleOption) IsOneWayMergeEnabled() bool {
	return o.Load().EnableOneWayMerge
}

// IsCrossTableMergeEnabled returns if across table merge is enabled.
func (o *ScheduleOption) IsCrossTableMergeEnabled() bool {
	return o.Load().EnableCrossTableMerge
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (o *ScheduleOption) GetPatrolRegionInterval() time.Duration {
	return o.Load().PatrolRegionInterval.Duration
}

// GetMaxStoreDownTime returns the max down time of a store.
func (o *ScheduleOption) GetMaxStoreDownTime() time.Duration {
	return o.Load().MaxStoreDownTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *ScheduleOption) GetLeaderScheduleLimit() uint64 {
	return o.Load().LeaderScheduleLimit
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *ScheduleOption) GetRegionScheduleLimit() uint64 {
	return o.Load().RegionScheduleLimit
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *ScheduleOption) GetReplicaScheduleLimit() uint64 {
	return o.Load().ReplicaScheduleLimit
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *ScheduleOption) GetMergeScheduleLimit() uint64 {
	return o.Load().MergeScheduleLimit
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (o *ScheduleOption) GetHotRegionScheduleLimit() uint64 {
	return o.Load().HotRegionScheduleLimit
}

// GetStoreBalanceRate returns the balance rate of a store.
func (o *ScheduleOption) GetStoreBalanceRate() float64 {
	return o.Load().StoreBalanceRate
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *ScheduleOption) GetTolerantSizeRatio() float64 {
	return o.Load().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *ScheduleOption) GetLowSpaceRatio() float64 {
	return o.Load().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *ScheduleOption) GetHighSpaceRatio() float64 {
	return o.Load().HighSpaceRatio
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *ScheduleOption) GetSchedulerMaxWaitingOperator() uint64 {
	return o.Load().SchedulerMaxWaitingOperator
}

// GetLeaderScheduleStrategy is to get leader schedule strategy.
func (o *ScheduleOption) GetLeaderScheduleStrategy() core.ScheduleStrategy {
	return core.StringToScheduleStrategy(o.Load().LeaderScheduleStrategy)
}

// GetKeyType is to get key type.
func (o *ScheduleOption) GetKeyType() core.KeyType {
	return core.StringToKeyType(o.LoadPDServerConfig().KeyType)
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *ScheduleOption) IsRemoveDownReplicaEnabled() bool {
	return o.Load().EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *ScheduleOption) IsReplaceOfflineReplicaEnabled() bool {
	return o.Load().EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *ScheduleOption) IsMakeUpReplicaEnabled() bool {
	return o.Load().EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *ScheduleOption) IsRemoveExtraReplicaEnabled() bool {
	return o.Load().EnableRemoveExtraReplica
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *ScheduleOption) IsLocationReplacementEnabled() bool {
	return o.Load().EnableLocationReplacement
}

// GetSchedulers gets the scheduler configurations.
func (o *ScheduleOption) GetSchedulers() SchedulerConfigs {
	return o.Load().Schedulers
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *ScheduleOption) AddSchedulerCfg(tp string, args []string) {
	c := o.Load()
	v := c.Clone()
	for i, schedulerCfg := range v.Schedulers {
		// comparing args is to cover the case that there are schedulers in same type but not with same name
		// such as two schedulers of type "evict-leader",
		// one name is "evict-leader-scheduler-1" and the other is "evict-leader-scheduler-2"
		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: false}) {
			return
		}

		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: true}) {
			schedulerCfg.Disable = false
			v.Schedulers[i] = schedulerCfg
			o.Store(v)
			return
		}
	}
	v.Schedulers = append(v.Schedulers, SchedulerConfig{Type: tp, Args: args, Disable: false})
	o.Store(v)
}

// RemoveSchedulerCfg removes the scheduler configurations.
func (o *ScheduleOption) RemoveSchedulerCfg(ctx context.Context, name string) error {
	c := o.Load()
	v := c.Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), decoder)
		if err != nil {
			return err
		}
		if tmp.GetName() == name {
			if IsDefaultScheduler(tmp.GetType()) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.Store(v)
			return nil
		}
	}
	return nil
}

// SetLabelProperty sets the label property.
func (o *ScheduleOption) SetLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.LoadLabelPropertyConfig().Clone()
	for _, l := range cfg[typ] {
		if l.Key == labelKey && l.Value == labelValue {
			return
		}
	}
	cfg[typ] = append(cfg[typ], StoreLabel{Key: labelKey, Value: labelValue})
	o.labelProperty.Store(cfg)
}

// DeleteLabelProperty deletes the label property.
func (o *ScheduleOption) DeleteLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.LoadLabelPropertyConfig().Clone()
	oldLabels := cfg[typ]
	cfg[typ] = []StoreLabel{}
	for _, l := range oldLabels {
		if l.Key == labelKey && l.Value == labelValue {
			continue
		}
		cfg[typ] = append(cfg[typ], l)
	}
	if len(cfg[typ]) == 0 {
		delete(cfg, typ)
	}
	o.labelProperty.Store(cfg)
}

// LoadLabelPropertyConfig returns the label property.
func (o *ScheduleOption) LoadLabelPropertyConfig() LabelPropertyConfig {
	return o.labelProperty.Load().(LabelPropertyConfig)
}

// SetClusterVersion sets the cluster version.
func (o *ScheduleOption) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// CASClusterVersion sets the cluster version.
func (o *ScheduleOption) CASClusterVersion(old, new *semver.Version) bool {
	return atomic.CompareAndSwapPointer(&o.clusterVersion, unsafe.Pointer(old), unsafe.Pointer(new))
}

// LoadClusterVersion returns the cluster version.
func (o *ScheduleOption) LoadClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// LoadPDServerConfig returns PD server configurations.
func (o *ScheduleOption) LoadPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// Persist saves the configuration to the storage.
func (o *ScheduleOption) Persist(storage *core.Storage) error {
	cfg := &Config{
		Schedule:       *o.Load(),
		Replication:    *o.replication.Load(),
		LabelProperty:  o.LoadLabelPropertyConfig(),
		ClusterVersion: *o.LoadClusterVersion(),
		PDServerCfg:    *o.LoadPDServerConfig(),
	}
	err := storage.SaveConfig(cfg)
	return err
}

// Reload reloads the configuration from the storage.
func (o *ScheduleOption) Reload(storage *core.Storage) error {
	cfg := &Config{
		Schedule:       *o.Load().Clone(),
		Replication:    *o.replication.Load(),
		LabelProperty:  o.LoadLabelPropertyConfig().Clone(),
		ClusterVersion: *o.LoadClusterVersion(),
		PDServerCfg:    *o.LoadPDServerConfig(),
	}
	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	o.adjustScheduleCfg(cfg)
	if isExist {
		o.Store(&cfg.Schedule)
		o.replication.Store(&cfg.Replication)
		o.labelProperty.Store(cfg.LabelProperty)
		o.SetClusterVersion(&cfg.ClusterVersion)
		o.pdServerConfig.Store(&cfg.PDServerCfg)
	}
	return nil
}

func (o *ScheduleOption) adjustScheduleCfg(persistentCfg *Config) {
	scheduleCfg := o.Load().Clone()
	for i, s := range scheduleCfg.Schedulers {
		for _, ps := range persistentCfg.Schedule.Schedulers {
			if s.Type == ps.Type && reflect.DeepEqual(s.Args, ps.Args) {
				scheduleCfg.Schedulers[i].Disable = ps.Disable
				break
			}
		}
	}
	restoredSchedulers := make([]SchedulerConfig, 0, len(persistentCfg.Schedule.Schedulers))
	for _, ps := range persistentCfg.Schedule.Schedulers {
		needRestore := true
		for _, s := range scheduleCfg.Schedulers {
			if s.Type == ps.Type && reflect.DeepEqual(s.Args, ps.Args) {
				needRestore = false
				break
			}
		}
		if needRestore {
			restoredSchedulers = append(restoredSchedulers, ps)
		}
	}
	scheduleCfg.Schedulers = append(scheduleCfg.Schedulers, restoredSchedulers...)
	persistentCfg.Schedule.Schedulers = scheduleCfg.Schedulers
	persistentCfg.Schedule.MigrateDeprecatedFlags()
	o.Store(scheduleCfg)
}

// GetHotRegionCacheHitsThreshold is a threshold to decide if a region is hot.
func (o *ScheduleOption) GetHotRegionCacheHitsThreshold() int {
	return int(o.Load().HotRegionCacheHitsThreshold)
}

// CheckLabelProperty checks the label property.
func (o *ScheduleOption) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	pc := o.labelProperty.Load().(LabelPropertyConfig)
	for _, cfg := range pc[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

// Replication provides some help to do replication.
type Replication struct {
	replicateCfg atomic.Value
}

func newReplication(cfg *ReplicationConfig) *Replication {
	r := &Replication{}
	r.Store(cfg)
	return r
}

// Load returns replication configurations.
func (r *Replication) Load() *ReplicationConfig {
	return r.replicateCfg.Load().(*ReplicationConfig)
}

// Store sets replication configurations.
func (r *Replication) Store(cfg *ReplicationConfig) {
	r.replicateCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (r *Replication) GetMaxReplicas() int {
	return int(r.Load().MaxReplicas)
}

// SetMaxReplicas set the replicas for each region.
func (r *Replication) SetMaxReplicas(replicas int) {
	c := r.Load()
	v := c.clone()
	v.MaxReplicas = uint64(replicas)
	r.Store(v)
}

// GetLocationLabels returns the location labels for each region.
func (r *Replication) GetLocationLabels() []string {
	return r.Load().LocationLabels
}

// GetStrictlyMatchLabel returns whether check label strict.
func (r *Replication) GetStrictlyMatchLabel() bool {
	return r.Load().StrictlyMatchLabel
}

// IsPlacementRulesEnabled returns whether the feature is enabled.
func (r *Replication) IsPlacementRulesEnabled() bool {
	return r.Load().EnablePlacementRules
}
