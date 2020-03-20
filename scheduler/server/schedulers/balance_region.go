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
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	allStores := cluster.GetStores()
	var stores []*core.StoreInfo

	for _, store := range allStores {
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		stores = append(stores, store)
	}

	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	for _, source := range stores {
		sourceID := source.GetID()

		for i := 0; i < balanceRegionRetryLimit; i++ {
			// Priority picks the region that has a pending peer.
			// Pending region may means the disk is overload, remove the pending region firstly.
			var region *core.RegionInfo
			cluster.GetPendingRegionsWithLock(sourceID, func(regions core.RegionsContainer) {
				region = regions.RandomRegion(nil, nil)
			})
			if region == nil {
				// Then picks the region that has a follower in the source store.
				cluster.GetFollowersWithLock(sourceID, func(regions core.RegionsContainer) {
					region = regions.RandomRegion(nil, nil)
				})
			}
			if region == nil {
				// Last, picks the region has the leader in the source store.
				cluster.GetLeadersWithLock(sourceID, func(regions core.RegionsContainer) {
					region = regions.RandomRegion(nil, nil)
				})
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
				return op
			}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID))
	}

	storeID := selectBestReplacementStore(cluster, region)
	if storeID == 0 {
		return nil
	}

	target := cluster.GetStore(storeID)
	if target == nil {
		log.Error("failed to get the target store", zap.Uint64("store-id", storeID))
		return nil
	}
	regionID := region.GetID()
	sourceID := source.GetID()
	targetID := target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	if int64(source.GetRegionSize()-target.GetRegionSize()) < 2*region.GetApproximateSize() {
		return nil
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

func selectBestReplacementStore(cluster opt.Cluster, region *core.RegionInfo) uint64 {
	var (
		best *core.StoreInfo
	)
	for _, store := range cluster.GetStores() {
		_, ok := region.GetStoreIds()[store.GetID()]
		if ok {
			continue
		}

		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}

		if best == nil || store.GetRegionSize() < best.GetRegionSize() {
			best = store
		}
	}
	if best == nil {
		return 0
	}
	return best.GetID()
}
