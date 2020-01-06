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
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/selector"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("shuffle-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler("shuffle-leader", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newShuffleLeaderScheduler(opController), nil
	})
}

const shuffleLeaderName = "shuffle-leader-scheduler"

type shuffleLeaderScheduler struct {
	name string
	*baseScheduler
	selector *selector.RandomSelector
}

// newShuffleLeaderScheduler creates an admin scheduler that shuffles leaders
// between stores.
func newShuffleLeaderScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: shuffleLeaderName, TransferLeader: true},
	}
	base := newBaseScheduler(opController)
	return &shuffleLeaderScheduler{
		name:          shuffleLeaderName,
		baseScheduler: base,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return s.name
}

func (s *shuffleLeaderScheduler) GetType() string {
	return "shuffle-leader"
}

func (s *shuffleLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	// We shuffle leaders between stores by:
	// 1. random select a valid store.
	// 2. transfer a leader to the store.
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	targetStore := s.selector.SelectTarget(cluster, stores)
	if targetStore == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
		return nil
	}
	region := cluster.RandFollowerRegion(targetStore.GetID(), core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op := operator.CreateTransferLeaderOperator("shuffle-leader", region, region.GetLeader().GetId(), targetStore.GetID(), operator.OpAdmin)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
