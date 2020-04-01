// Copyright 2016 PingCAP, Inc.
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

package selector

import (
	"math/rand"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

// ReplicaSelector selects source/target store candidates based on their
// distinct scores based on a region's peer stores.
type ReplicaSelector struct {
	regionStores []*core.StoreInfo
	filters      []filter.Filter
}

// NewReplicaSelector creates a ReplicaSelector instance.
func NewReplicaSelector(regionStores []*core.StoreInfo, filters ...filter.Filter) *ReplicaSelector {
	return &ReplicaSelector{
		regionStores: regionStores,
		filters:      filters,
	}
}

// SelectSource selects the store that can pass all filters and has the minimal
// distinct score.
func (s *ReplicaSelector) SelectSource(opt opt.Options, stores []*core.StoreInfo) *core.StoreInfo {
	var (
		best *core.StoreInfo
	)
	for _, store := range stores {
		if best == nil || compareStoreScore(store, best) < 0 {
			best = store
		}
	}
	if best == nil || filter.Source(opt, best, s.filters) {
		return nil
	}
	return best
}

// SelectTarget selects the store that can pass all filters and has the maximal
// distinct score.
func (s *ReplicaSelector) SelectTarget(opt opt.Options, stores []*core.StoreInfo, filters ...filter.Filter) *core.StoreInfo {
	var (
		best *core.StoreInfo
	)
	for _, store := range stores {
		if filter.Target(opt, store, filters) {
			continue
		}
		if best == nil || compareStoreScore(store, best) > 0 {
			best = store
		}
	}
	if best == nil || filter.Target(opt, best, s.filters) {
		return nil
	}
	return best
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(storeA *core.StoreInfo, storeB *core.StoreInfo) int {
	// The store with lower region score is better.
	if storeA.GetRegionSize() <
		storeB.GetRegionSize() {
		return 1
	}
	if storeA.GetRegionSize() >
		storeB.GetRegionSize() {
		return -1
	}
	return 0
}

// RandomSelector selects source/target store randomly.
type RandomSelector struct {
	filters []filter.Filter
}

func (s *RandomSelector) randStore(stores []*core.StoreInfo) *core.StoreInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

// SelectSource randomly selects a source store from those can pass all filters.
func (s *RandomSelector) SelectSource(opt opt.Options, stores []*core.StoreInfo) *core.StoreInfo {
	var candidates []*core.StoreInfo
	for _, store := range stores {
		if filter.Source(opt, store, s.filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.randStore(candidates)
}

// SelectTarget randomly selects a target store from those can pass all filters.
func (s *RandomSelector) SelectTarget(opt opt.Options, stores []*core.StoreInfo, filters ...filter.Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if filter.Target(opt, store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.randStore(candidates)
}
