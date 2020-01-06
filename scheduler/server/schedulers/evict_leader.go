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

package schedulers

import (
	"fmt"
	"strconv"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/selector"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("evict-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			name := fmt.Sprintf("evict-leader-scheduler-%d", id)
			conf.StoreID = id
			conf.Name = name
			return nil

		}
	})

	schedule.RegisterScheduler("evict-leader", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{}
		decoder(conf)
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

type evictLeaderSchedulerConfig struct {
	Name    string `json:"name"`
	StoreID uint64 `json:"store-id"`
}

type evictLeaderScheduler struct {
	*baseScheduler
	conf     *evictLeaderSchedulerConfig
	selector *selector.RandomSelector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: conf.Name, TransferLeader: true},
	}

	base := newBaseScheduler(opController)
	return &evictLeaderScheduler{
		baseScheduler: base,
		conf:          conf,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.conf.Name
}

func (s *evictLeaderScheduler) GetType() string {
	return "evict-leader"
}

func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *evictLeaderScheduler) Prepare(cluster opt.Cluster) error {
	return cluster.BlockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	cluster.UnblockStore(s.conf.StoreID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandLeaderRegion(s.conf.StoreID, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator("evict-leader", region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
