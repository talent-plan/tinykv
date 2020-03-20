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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-leader", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler("balance-leader", func(opController *schedule.OperatorController, storage *core.Storage, mapper schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceLeaderScheduler(opController), nil
	})
}

// balanceLeaderRetryLimit is the limit to retry schedule for selected source store and target store.
const balanceLeaderRetryLimit = 10

type balanceLeaderScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
	filters      []filter.Filter
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newBalanceLeaderScheduler(opController *schedule.OperatorController, opts ...BalanceLeaderCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)

	s := &balanceLeaderScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true}}
	return s
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

func (l *balanceLeaderScheduler) GetName() string {
	if l.name != "" {
		return l.name
	}
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetType() string {
	return "balance-leader"
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (l *balanceLeaderScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, l.filters, cluster)
	targets := filter.SelectTargetStores(stores, l.filters, cluster)
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetLeaderCount() > sources[j].GetLeaderCount()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetLeaderCount() < targets[j].GetLeaderCount()
	})

	for i := 0; i < len(sources) || i < len(targets); i++ {
		if i < len(sources) {
			source := sources[i]
			sourceID := source.GetID()
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("source-store", sourceID))
			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if op := l.transferLeaderOut(cluster, source); op != nil {
					return op
				}
			}
			log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("source", sourceID))
		}
		if i < len(targets) {
			target := targets[i]
			targetID := target.GetID()
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("target-store", targetID))

			for j := 0; j < balanceLeaderRetryLimit; j++ {
				if op := l.transferLeaderIn(cluster, target); op != nil {
					return op
				}
			}
			log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("target", targetID))
		}
	}
	return nil
}

// transferLeaderOut transfers leader from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(cluster opt.Cluster, source *core.StoreInfo) *operator.Operator {
	sourceID := source.GetID()
	region := cluster.RandLeaderRegion(sourceID, core.HealthRegion())
	if region == nil {
		log.Debug("store has no leader", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", sourceID))
		return nil
	}
	targets := cluster.GetFollowerStores(region)
	targets = filter.SelectTargetStores(targets, l.filters, cluster)
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetLeaderCount() < targets[j].GetLeaderCount()
	})
	for _, target := range targets {
		if op := l.createOperator(cluster, region, source, target); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", region.GetID()))
	return nil
}

// transferLeaderIn transfers leader to the target store.
// It randomly selects a health region from the target store, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(cluster opt.Cluster, target *core.StoreInfo) *operator.Operator {
	targetID := target.GetID()
	region := cluster.RandFollowerRegion(targetID, core.HealthRegion())
	if region == nil {
		log.Debug("store has no follower", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", targetID))
		return nil
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	source := cluster.GetStore(leaderStoreID)
	if source == nil {
		log.Debug("region has no leader or leader store cannot be found",
			zap.String("scheduler", l.GetName()),
			zap.Uint64("region-id", region.GetID()),
			zap.Uint64("store-id", leaderStoreID),
		)
		return nil
	}
	return l.createOperator(cluster, region, source, target)
}

// createOperator creates the operator according to the source and target store.
// If the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (l *balanceLeaderScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) *operator.Operator {
	targetID := target.GetID()

	if source.GetLeaderCount()-target.GetLeaderCount() < 2*int(1.0*leaderTolerantSizeRatio) {
		return nil
	}

	op := operator.CreateTransferLeaderOperator("balance-leader", region, region.GetLeader().GetStoreId(), targetID, operator.OpBalance)
	return op
}
