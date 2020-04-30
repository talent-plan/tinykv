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
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/selector"
	"github.com/pingcap/log"
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
		filter.NewHealthFilter(name),
	}

	return &ReplicaChecker{
		name:    name,
		cluster: cluster,
		filters: filters,
	}
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	if op := r.checkOfflinePeer(region); op != nil {
		op.SetPriorityLevel(core.HighPriority)
		return op
	}

	if len(region.GetPeers()) < r.cluster.GetMaxReplicas() {
		log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		newPeer := r.selectBestPeerToAddReplica(region)
		if newPeer == nil {
			return nil
		}
		return operator.CreateAddPeerOperator("make-up-replica", region, newPeer.GetId(), newPeer.GetStoreId(), operator.OpReplica)
	}

	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) > r.cluster.GetMaxReplicas() {
		log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		oldPeer := r.selectWorstPeer(region)
		if oldPeer == nil {
			return nil
		}
		op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, region, oldPeer.GetStoreId())
		if err != nil {
			return nil
		}
		return op
	}

	return nil
}

// SelectBestReplacementStore returns a store id that to be used to replace the old peer and distinct score.
func (r *ReplicaChecker) SelectBestReplacementStore(region *core.RegionInfo, oldPeer *metapb.Peer, filters ...filter.Filter) uint64 {
	filters = append(filters, filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()))
	newRegion := region.Clone(core.WithRemoveStorePeer(oldPeer.GetStoreId()))
	return r.selectBestStoreToAddReplica(newRegion, filters...)
}

// selectBestPeerToAddReplica returns a new peer that to be used to add a replica and distinct score.
func (r *ReplicaChecker) selectBestPeerToAddReplica(region *core.RegionInfo, filters ...filter.Filter) *metapb.Peer {
	storeID := r.selectBestStoreToAddReplica(region, filters...)
	if storeID == 0 {
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	return newPeer
}

// selectBestStoreToAddReplica returns the store to add a replica.
func (r *ReplicaChecker) selectBestStoreToAddReplica(region *core.RegionInfo, filters ...filter.Filter) uint64 {
	// Add some must have filters.
	newFilters := []filter.Filter{
		filter.NewStateFilter(r.name),
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
	}
	filters = append(filters, r.filters...)
	filters = append(filters, newFilters...)
	regionStores := r.cluster.GetRegionStores(region)
	s := selector.NewReplicaSelector(regionStores, r.filters...)
	target := s.SelectTarget(r.cluster, r.cluster.GetStores(), filters...)
	if target == nil {
		return 0
	}
	return target.GetID()
}

// selectWorstPeer returns the worst peer in the region.
func (r *ReplicaChecker) selectWorstPeer(region *core.RegionInfo) *metapb.Peer {
	regionStores := r.cluster.GetRegionStores(region)
	s := selector.NewReplicaSelector(regionStores, r.filters...)
	worstStore := s.SelectSource(r.cluster, regionStores)
	if worstStore == nil {
		log.Debug("no worst store", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	return region.GetStorePeer(worstStore.GetID())
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
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

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, peer *metapb.Peer, status string) *operator.Operator {
	removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
	// Check the number of replicas first.
	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, region, peer.GetStoreId())
		if err != nil {
			return nil
		}
		return op
	}

	storeID := r.SelectBestReplacementStore(region, peer)
	if storeID == 0 {
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
		return nil
	}
	return op
}
