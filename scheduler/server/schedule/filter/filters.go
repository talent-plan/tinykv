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

package filter

import (
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/cache"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/slice"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

//revive:disable:unused-parameter

// SelectSourceStores selects stores that be selected as source store from the list.
func SelectSourceStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.NoneOf(filters, func(i int) bool { return filters[i].Source(opt, s) })
	})
}

// SelectTargetStores selects stores that be selected as target store from the list.
func SelectTargetStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.NoneOf(filters, func(i int) bool { return filters[i].Target(opt, s) })
	})
}

func filterStoresBy(stores []*core.StoreInfo, keepPred func(*core.StoreInfo) bool) (selected []*core.StoreInfo) {
	for _, s := range stores {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// Filter is an interface to filter source and target store.
type Filter interface {
	// Scope is used to indicate where the filter will act on.
	Scope() string
	Type() string
	// Return true if the store should not be used as a source store.
	Source(opt opt.Options, store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	Target(opt opt.Options, store *core.StoreInfo) bool
}

// Source checks if store can pass all Filters as source store.
func Source(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.Source(opt, store) {
			return true
		}
	}
	return false
}

// Target checks if store can pass all Filters as target store.
func Target(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.Target(opt, store) {
			return true
		}
	}
	return false
}

type excludedFilter struct {
	scope   string
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(scope string, sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		scope:   scope,
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Scope() string {
	return f.scope
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return ok
}

func (f *excludedFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return ok
}

type storeLimitFilter struct{ scope string }

// NewStoreLimitFilter creates a Filter that filters all stores those exceed the limit of a store.
func NewStoreLimitFilter(scope string) Filter {
	return &storeLimitFilter{scope: scope}
}

func (f *storeLimitFilter) Scope() string {
	return f.scope
}

func (f *storeLimitFilter) Type() string {
	return "store-limit-filter"
}

func (f *storeLimitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsAvailable()
}

func (f *storeLimitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsAvailable()
}

type stateFilter struct{ scope string }

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter(scope string) Filter {
	return &stateFilter{scope: scope}
}

func (f *stateFilter) Scope() string {
	return f.scope
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f *stateFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsUp()
}

type healthFilter struct{ scope string }

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter(scope string) Filter {
	return &healthFilter{scope: scope}
}

func (f *healthFilter) Scope() string {
	return f.scope
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsBusy() {
		return true
	}
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f *healthFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type pendingPeerCountFilter struct{ scope string }

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter(scope string) Filter {
	return &pendingPeerCountFilter{scope: scope}
}

func (p *pendingPeerCountFilter) Scope() string {
	return p.scope
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return false
	}
	return store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{ scope string }

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter(scope string) Filter {
	return &snapshotCountFilter{scope: scope}
}

func (f *snapshotCountFilter) Scope() string {
	return f.scope
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type cacheFilter struct {
	scope string
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(scope string, cache *cache.TTLUint64) Filter {
	return &cacheFilter{scope: scope, cache: cache}
}

func (f *cacheFilter) Scope() string {
	return f.scope
}

func (f *cacheFilter) Type() string {
	return "cache-filter"
}

func (f *cacheFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetID())
}

func (f *cacheFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{ scope string }

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(scope string) Filter {
	return &storageThresholdFilter{scope: scope}
}

func (f *storageThresholdFilter) Scope() string {
	return f.scope
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		scope:     scope,
		labels:    labels,
		stores:    newStores,
		safeScore: core.DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return core.DistinctScore(f.labels, f.stores, store) < f.safeScore
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// Source returns true when the store cannot be selected as the schedule
// source.
func (f StoreStateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return true
	}

	if f.MoveRegion && f.filterMoveRegion(opt, store) {
		return true
	}
	return false
}

// Target returns true when the store cannot be selected as the schedule
// target.
func (f StoreStateFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opts.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.IsBusy() ||
			opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())) {
		return true
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opts.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opts.GetMaxPendingPeerCount()) {
			return true
		}

		if f.filterMoveRegion(opts, store) {
			return true
		}
	}
	return false
}

func (f StoreStateFilter) filterMoveRegion(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsBusy() {
		return true
	}

	if !store.IsAvailable() {
		return true
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return true
	}
	return false
}

// BlacklistType the type of BlackListStore Filter.
type BlacklistType int

// some flags about blacklist type.
const (
	// blacklist associated with the source.
	BlacklistSource BlacklistType = 1 << iota
	// blacklist associated with the target.
	BlacklistTarget
)

// BlacklistStoreFilter filters the store according to the blacklist.
type BlacklistStoreFilter struct {
	scope     string
	blacklist map[uint64]struct{}
	flag      BlacklistType
}

// NewBlacklistStoreFilter creates a blacklist filter.
func NewBlacklistStoreFilter(scope string, typ BlacklistType) *BlacklistStoreFilter {
	return &BlacklistStoreFilter{
		scope:     scope,
		blacklist: make(map[uint64]struct{}),
		flag:      typ,
	}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *BlacklistStoreFilter) Scope() string {
	return f.scope
}

// Type implements the Filter.
func (f *BlacklistStoreFilter) Type() string {
	return "blacklist-store-filter"
}

// Source implements the Filter.
func (f *BlacklistStoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistSource != BlacklistSource {
		return false
	}
	return f.filter(store)
}

// Add adds the store to the blacklist.
func (f *BlacklistStoreFilter) Add(storeID uint64) {
	f.blacklist[storeID] = struct{}{}
}

// Target implements the Filter.
func (f *BlacklistStoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistTarget != BlacklistTarget {
		return false
	}
	return f.filter(store)
}

func (f *BlacklistStoreFilter) filter(store *core.StoreInfo) bool {
	_, ok := f.blacklist[store.GetID()]
	return ok
}
