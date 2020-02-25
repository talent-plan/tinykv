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

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
)

// ScheduleOption is a wrapper to access the configuration safely.
type ScheduleOption struct {
	schedule       atomic.Value
	replication    *Replication
	pdServerConfig atomic.Value
}

// NewScheduleOption creates a new ScheduleOption.
func NewScheduleOption(cfg *Config) *ScheduleOption {
	o := &ScheduleOption{}
	o.Store(&cfg.Schedule)
	o.replication = newReplication(&cfg.Replication)
	o.pdServerConfig.Store(&cfg.PDServerCfg)
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

// GetMaxReplicas returns the number of replicas for each region.
func (o *ScheduleOption) GetMaxReplicas() int {
	return o.replication.GetMaxReplicas()
}

// SetMaxReplicas sets the number of replicas for each region.
func (o *ScheduleOption) SetMaxReplicas(replicas int) {
	o.replication.SetMaxReplicas(replicas)
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

// LoadPDServerConfig returns PD server configurations.
func (o *ScheduleOption) LoadPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
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
