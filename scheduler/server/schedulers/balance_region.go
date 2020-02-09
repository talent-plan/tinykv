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

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/checker"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
	filters      []filter.Filter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true}}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

// WithBalanceRegionName sets the name for the scheduler.
func WithBalanceRegionName(name string) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.name = name
	}
}

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	stores := cluster.GetStores()
	stores = filter.SelectSourceStores(stores, s.filters, cluster)
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0) > stores[j].RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)
	})
	for _, source := range stores {
		sourceID := source.GetID()

		for i := 0; i < balanceRegionRetryLimit; i++ {
			// Priority picks the region that has a pending peer.
			// Pending region may means the disk is overload, remove the pending region firstly.
			region := cluster.RandPendingRegion(sourceID, core.HealthRegionAllowPending())
			if region == nil {
				// Then picks the region that has a follower in the source store.
				region = cluster.RandFollowerRegion(sourceID, core.HealthRegion())
			}
			if region == nil {
				// Last, picks the region has the leader in the source store.
				region = cluster.RandLeaderRegion(sourceID, core.HealthRegion())
			}
			if region == nil {
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))

			// We don't schedule region with abnormal number of replicas.
			if len(region.GetPeers()) != cluster.GetMaxReplicas() {
				log.Debug("region has abnormal replica count", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				continue
			}

			oldPeer := region.GetStorePeer(sourceID)
			if op := s.transferPeer(cluster, region, oldPeer); op != nil {
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID))
	}
	scoreGuard := filter.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), stores, source)
	checker := checker.NewReplicaChecker(cluster, s.GetName())
	exclude := make(map[uint64]struct{})
	excludeFilter := filter.NewExcludedFilter(s.name, nil, exclude)
	for {
		storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard, excludeFilter)
		if storeID == 0 {
			return nil
		}
		exclude[storeID] = struct{}{} // exclude next round.

		target := cluster.GetStore(storeID)
		if target == nil {
			log.Error("failed to get the target store", zap.Uint64("store-id", storeID))
			continue
		}
		regionID := region.GetID()
		sourceID := source.GetID()
		targetID := target.GetID()
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		opInfluence := s.opController.GetOpInfluence(cluster)
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		if !shouldBalance(cluster, source, target, region, kind, opInfluence, s.GetName()) {
			continue
		}

		newPeer, err := cluster.AllocPeer(storeID)
		if err != nil {
			return nil
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
		if err != nil {
			return nil
		}
		return op
	}
}
