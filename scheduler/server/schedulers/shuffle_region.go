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
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/selector"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("shuffle-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("shuffle-region", func(opController *schedule.OperatorController, straoge *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newShuffleRegionScheduler(opController), nil
	})
}

const shuffleRegionName = "shuffle-region-scheduler"

type shuffleRegionScheduler struct {
	name string
	*baseScheduler
	selector *selector.RandomSelector
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: shuffleRegionName, MoveRegion: true},
	}
	base := newBaseScheduler(opController)
	return &shuffleRegionScheduler{
		name:          shuffleRegionName,
		baseScheduler: base,
		selector:      selector.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return s.name
}

func (s *shuffleRegionScheduler) GetType() string {
	return "shuffle-region"
}

func (s *shuffleRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *shuffleRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := s.scheduleRemovePeer(cluster)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil
	}

	excludedFilter := filter.NewExcludedFilter(s.GetName(), nil, region.GetStoreIds())
	newPeer := s.scheduleAddPeer(cluster, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-new-peer").Inc()
		return nil
	}

	op, err := operator.CreateMovePeerOperator("shuffle-region", cluster, region, operator.OpAdmin, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}

func (s *shuffleRegionScheduler) scheduleRemovePeer(cluster opt.Cluster) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()

	source := s.selector.SelectSource(cluster, stores)
	if source == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-source-store").Inc()
		return nil, nil
	}

	region := cluster.RandFollowerRegion(source.GetID(), core.HealthRegion())
	if region == nil {
		region = cluster.RandLeaderRegion(source.GetID(), core.HealthRegion())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetID())
}

func (s *shuffleRegionScheduler) scheduleAddPeer(cluster opt.Cluster, filter filter.Filter) *metapb.Peer {
	stores := cluster.GetStores()

	target := s.selector.SelectTarget(cluster, stores, filter)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		log.Error("failed to allocate peer", zap.Error(err))
		return nil
	}

	return newPeer
}
