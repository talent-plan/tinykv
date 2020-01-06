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

package checker

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/selector"
	"go.uber.org/zap"
)

const replicaCheckerName = "replica-checker"

const (
	offlineStatus = "offline"
	downStatus    = "down"
)

// ReplicaChecker ensures region has the best replicas.
// Including the following:
// Replica number management.
// Unhealthy replica management, mainly used for disaster recovery of TiKV.
// Location management, mainly used for cross data center deployment.
type ReplicaChecker struct {
	name    string
	cluster opt.Cluster
	filters []filter.Filter
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster opt.Cluster, n ...string) *ReplicaChecker {
	name := replicaCheckerName
	if len(n) != 0 {
		name = n[0]
	}
	filters := []filter.Filter{
		filter.NewStoreLimitFilter(name),
		filter.NewHealthFilter(name),
		filter.NewSnapshotCountFilter(name),
		filter.NewPendingPeerCountFilter(name),
	}

	return &ReplicaChecker{
		name:    name,
		cluster: cluster,
		filters: filters,
	}
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	if op := r.checkDownPeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}

	if len(region.GetPeers()) < r.cluster.GetMaxReplicas() && r.cluster.IsMakeUpReplicaEnabled() {
		log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		newPeer, _ := r.selectBestPeerToAddReplica(region, filter.NewStorageThresholdFilter(r.name))
		if newPeer == nil {
			checkerCounter.WithLabelValues("replica_checker", "no-target-store").Inc()
			return nil
		}
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return operator.CreateAddPeerOperator("make-up-replica", region, newPeer.GetId(), newPeer.GetStoreId(), operator.OpReplica)
	}

	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) > r.cluster.GetMaxReplicas() && r.cluster.IsRemoveExtraReplicaEnabled() {
		log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			checkerCounter.WithLabelValues("replica_checker", "no-worst-peer").Inc()
			return nil
		}
		op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, region, oldPeer.GetStoreId())
		if err != nil {
			checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
			return nil
		}
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}

	return r.checkBestReplacement(region)
}

// SelectBestReplacementStore returns a store id that to be used to replace the old peer and distinct score.
func (r *ReplicaChecker) SelectBestReplacementStore(region *core.RegionInfo, oldPeer *metapb.Peer, filters ...filter.Filter) (uint64, float64) {
	filters = append(filters, filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()))
	newRegion := region.Clone(core.WithRemoveStorePeer(oldPeer.GetStoreId()))
	return r.selectBestStoreToAddReplica(newRegion, filters...)
}

// selectBestPeerToAddReplica returns a new peer that to be used to add a replica and distinct score.
func (r *ReplicaChecker) selectBestPeerToAddReplica(region *core.RegionInfo, filters ...filter.Filter) (*metapb.Peer, float64) {
	storeID, score := r.selectBestStoreToAddReplica(region, filters...)
	if storeID == 0 {
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil, 0
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil, 0
	}
	return newPeer, score
}

// selectBestStoreToAddReplica returns the store to add a replica.
func (r *ReplicaChecker) selectBestStoreToAddReplica(region *core.RegionInfo, filters ...filter.Filter) (uint64, float64) {
	// Add some must have filters.
	newFilters := []filter.Filter{
		filter.NewStateFilter(r.name),
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
	}
	filters = append(filters, r.filters...)
	filters = append(filters, newFilters...)
	regionStores := r.cluster.GetRegionStores(region)
	s := selector.NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	target := s.SelectTarget(r.cluster, r.cluster.GetStores(), filters...)
	if target == nil {
		return 0, 0
	}
	return target.GetID(), core.DistinctScore(r.cluster.GetLocationLabels(), regionStores, target)
}

// selectWorstPeer returns the worst peer in the region.
func (r *ReplicaChecker) selectWorstPeer(region *core.RegionInfo) (*metapb.Peer, float64) {
	regionStores := r.cluster.GetRegionStores(region)
	s := selector.NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	worstStore := s.SelectSource(r.cluster, regionStores)
	if worstStore == nil {
		log.Debug("no worst store", zap.Uint64("region-id", region.GetID()))
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetID()), core.DistinctScore(r.cluster.GetLocationLabels(), regionStores, worstStore)
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range region.GetDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(region, peer, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.IsUp() {
			continue
		}

		return r.fixPeer(region, peer, offlineStatus)
	}

	return nil
}

func (r *ReplicaChecker) checkBestReplacement(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsLocationReplacementEnabled() {
		return nil
	}

	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		checkerCounter.WithLabelValues("replica_checker", "all-right").Inc()
		return nil
	}
	storeID, newScore := r.SelectBestReplacementStore(region, oldPeer, filter.NewStorageThresholdFilter(r.name))
	if storeID == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no-replacement-store").Inc()
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()), zap.Float64("new-score", newScore), zap.Float64("old-score", oldScore))
		checkerCounter.WithLabelValues("replica_checker", "not-better").Inc()
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, region, operator.OpReplica, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
	return op
}

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, peer *metapb.Peer, status string) *operator.Operator {
	removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
	// Check the number of replicas first.
	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, region, peer.GetStoreId())
		if err != nil {
			reason := fmt.Sprintf("%s-fail", removeExtra)
			checkerCounter.WithLabelValues("replica_checker", reason).Inc()
			return nil
		}
		return op
	}

	storeID, _ := r.SelectBestReplacementStore(region, peer, filter.NewStorageThresholdFilter(r.name))
	if storeID == 0 {
		reason := fmt.Sprintf("no-store-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}

	replace := fmt.Sprintf("replace-%s-replica", status)
	var op *operator.Operator
	if status == offlineStatus {
		op, err = operator.CreateOfflinePeerOperator(replace, r.cluster, region, operator.OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	} else {
		op, err = operator.CreateMovePeerOperator(replace, r.cluster, region, operator.OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	}
	if err != nil {
		reason := fmt.Sprintf("%s-fail", replace)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		return nil
	}
	return op
}
