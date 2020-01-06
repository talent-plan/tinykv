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
	"bytes"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/selector"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	scanLimit                    = 1000
	defaultAdjacentPeerLimit     = 1
	defaultAdjacentLeaderLimit   = 64
	minAdjacentSchedulerInterval = time.Second
	maxAdjacentSchedulerInterval = 30 * time.Second
	balanceAdjacentRegionName    = "balance-adjacent-region-scheduler"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("adjacent-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceAdjacentRegionConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			if len(args) == 2 {
				leaderLimit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errors.WithStack(err)
				}
				peerLimit, err := strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					return errors.WithStack(err)
				}
				conf.LeaderLimit = leaderLimit
				conf.PeerLimit = peerLimit
				return nil
			}
			conf.LeaderLimit = defaultAdjacentLeaderLimit
			conf.PeerLimit = defaultAdjacentPeerLimit
			return nil
		}
	})

	schedule.RegisterScheduler("adjacent-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceAdjacentRegionConfig{
			LeaderLimit: defaultAdjacentLeaderLimit,
			PeerLimit:   defaultAdjacentPeerLimit,
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceAdjacentRegionScheduler(opController, conf), nil
	})
}

type balanceAdjacentRegionConfig struct {
	LeaderLimit uint64 `json:"leader-limit"`
	PeerLimit   uint64 `json:"peer-limit"`
}

// balanceAdjacentRegionScheduler will disperse adjacent regions.
// we will scan a part regions order by key, then select the longest
// adjacent regions and disperse them. finally, we will guarantee
// 1. any two adjacent regions' leader will not in the same store
// 2. the two regions' leader will not in the public store of this two regions
type balanceAdjacentRegionScheduler struct {
	*baseScheduler
	selector             *selector.RandomSelector
	lastKey              []byte
	cacheRegions         *adjacentState
	conf                 *balanceAdjacentRegionConfig
	adjacentRegionsCount int
}

type adjacentState struct {
	assignedStoreIds []uint64
	regions          []*core.RegionInfo
	head             int
}

func (a *adjacentState) clear() {
	a.assignedStoreIds = a.assignedStoreIds[:0]
	a.regions = a.regions[:0]
	a.head = 0
}

func (a *adjacentState) len() int {
	return len(a.regions) - a.head
}

// newBalanceAdjacentRegionScheduler creates a scheduler that tends to disperse adjacent region
// on each store.
func newBalanceAdjacentRegionScheduler(opController *schedule.OperatorController, conf *balanceAdjacentRegionConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: balanceAdjacentRegionName, TransferLeader: true, MoveRegion: true},
	}
	base := newBaseScheduler(opController)
	s := &balanceAdjacentRegionScheduler{
		baseScheduler: base,
		selector:      selector.NewRandomSelector(filters),
		conf:          conf,
		lastKey:       []byte(""),
	}
	return s
}

func (l *balanceAdjacentRegionScheduler) GetName() string {
	return balanceAdjacentRegionName
}

func (l *balanceAdjacentRegionScheduler) GetType() string {
	return "adjacent-region"
}

func (l *balanceAdjacentRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceAdjacentRegionScheduler) GetMinInterval() time.Duration {
	return minAdjacentSchedulerInterval
}

func (l *balanceAdjacentRegionScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, maxAdjacentSchedulerInterval, linearGrowth)
}

func (l *balanceAdjacentRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return l.allowBalanceLeader() || l.allowBalancePeer()
}

func (l *balanceAdjacentRegionScheduler) allowBalanceLeader() bool {
	return l.opController.OperatorCount(operator.OpAdjacent|operator.OpLeader) < l.conf.LeaderLimit
}

func (l *balanceAdjacentRegionScheduler) allowBalancePeer() bool {
	return l.opController.OperatorCount(operator.OpAdjacent|operator.OpRegion) < l.conf.PeerLimit
}

func (l *balanceAdjacentRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	if l.cacheRegions == nil {
		l.cacheRegions = &adjacentState{
			assignedStoreIds: make([]uint64, 0, len(cluster.GetStores())),
			regions:          make([]*core.RegionInfo, 0, scanLimit),
			head:             0,
		}
	}
	// we will process cache firstly
	if l.cacheRegions.len() >= 2 {
		return l.process(cluster)
	}

	l.cacheRegions.clear()
	regions := cluster.ScanRegions(l.lastKey, nil, scanLimit)
	// scan to the end
	if len(regions) <= 1 {
		schedulerStatus.WithLabelValues(l.GetName(), "adjacent-count").Set(float64(l.adjacentRegionsCount))
		l.adjacentRegionsCount = 0
		l.lastKey = []byte("")
		return nil
	}

	// calculate max adjacentRegions and record to the cache
	adjacentRegions := make([]*core.RegionInfo, 0, scanLimit)
	adjacentRegions = append(adjacentRegions, regions[0])
	maxLen := 0
	for i, r := range regions[1:] {
		l.lastKey = r.GetStartKey()

		// append if the region are adjacent
		lastRegion := adjacentRegions[len(adjacentRegions)-1]
		if lastRegion.GetLeader().GetStoreId() == r.GetLeader().GetStoreId() && bytes.Equal(lastRegion.GetEndKey(), r.GetStartKey()) {
			adjacentRegions = append(adjacentRegions, r)
			if i != len(regions)-2 { // not the last element
				continue
			}
		}

		if len(adjacentRegions) == 1 {
			adjacentRegions[0] = r
		} else {
			// got an max length adjacent regions in this range
			if maxLen < len(adjacentRegions) {
				l.cacheRegions.clear()
				maxLen = len(adjacentRegions)
				l.cacheRegions.regions = append(l.cacheRegions.regions, adjacentRegions...)
				adjacentRegions = adjacentRegions[:0]
				adjacentRegions = append(adjacentRegions, r)
			}
		}
	}

	l.adjacentRegionsCount += maxLen
	return l.process(cluster)
}

func (l *balanceAdjacentRegionScheduler) process(cluster opt.Cluster) []*operator.Operator {
	if l.cacheRegions.len() < 2 {
		return nil
	}
	head := l.cacheRegions.head
	r1 := l.cacheRegions.regions[head]
	r2 := l.cacheRegions.regions[head+1]

	defer func() {
		if l.cacheRegions.len() < 0 {
			log.Fatal("cache overflow", zap.String("scheduler", l.GetName()))
		}
		l.cacheRegions.head = head + 1
		l.lastKey = r2.GetStartKey()
	}()
	// after the cluster is prepared, there is a gap that some regions heartbeats are not received.
	// Leader of those region is nil, and we should skip them.
	if r1.GetLeader() == nil || r2.GetLeader() == nil || l.unsafeToBalance(cluster, r1) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}
	op := l.disperseLeader(cluster, r1, r2)
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		op = l.dispersePeer(cluster, r1)
	}
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no-peer").Inc()
		l.cacheRegions.assignedStoreIds = l.cacheRegions.assignedStoreIds[:0]
		return nil
	}
	return []*operator.Operator{op}
}

func (l *balanceAdjacentRegionScheduler) unsafeToBalance(cluster opt.Cluster, region *core.RegionInfo) bool {
	if len(region.GetPeers()) != cluster.GetMaxReplicas() {
		return true
	}
	storeID := region.GetLeader().GetStoreId()
	store := cluster.GetStore(storeID)
	if store == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID))
		return true
	}
	s := l.selector.SelectSource(cluster, []*core.StoreInfo{store})
	if s == nil {
		return true
	}
	// Skip hot regions.
	if cluster.IsRegionHot(region) {
		schedulerCounter.WithLabelValues(l.GetName(), "region-hot").Inc()
		return true
	}
	return false
}

func (l *balanceAdjacentRegionScheduler) disperseLeader(cluster opt.Cluster, before *core.RegionInfo, after *core.RegionInfo) *operator.Operator {
	if !l.allowBalanceLeader() {
		return nil
	}
	diffPeers := before.GetDiffFollowers(after)
	if len(diffPeers) == 0 {
		return nil
	}
	storesInfo := make([]*core.StoreInfo, 0, len(diffPeers))
	for _, p := range diffPeers {
		if store := cluster.GetStore(p.GetStoreId()); store != nil {
			storesInfo = append(storesInfo, store)
		}
	}
	target := l.selector.SelectTarget(cluster, storesInfo)
	if target == nil {
		return nil
	}
	op := operator.CreateTransferLeaderOperator("balance-adjacent-leader", before, before.GetLeader().GetStoreId(), target.GetID(), operator.OpAdjacent)
	op.SetPriorityLevel(core.LowPriority)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent-leader").Inc()
	return op
}

func (l *balanceAdjacentRegionScheduler) dispersePeer(cluster opt.Cluster, region *core.RegionInfo) *operator.Operator {
	if !l.allowBalancePeer() {
		return nil
	}
	// scoreGuard guarantees that the distinct score will not decrease.
	leaderStoreID := region.GetLeader().GetStoreId()
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(leaderStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", leaderStoreID))
		return nil
	}

	scoreGuard := filter.NewDistinctScoreFilter(l.GetName(), cluster.GetLocationLabels(), stores, source)
	excludeStores := region.GetStoreIds()
	for _, storeID := range l.cacheRegions.assignedStoreIds {
		if _, ok := excludeStores[storeID]; !ok {
			excludeStores[storeID] = struct{}{}
		}
	}

	filters := []filter.Filter{
		filter.NewExcludedFilter(l.GetName(), nil, excludeStores),
		scoreGuard,
	}
	target := l.selector.SelectTarget(cluster, cluster.GetStores(), filters...)
	if target == nil {
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}
	if newPeer == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no-peer").Inc()
		return nil
	}

	// record the store id and exclude it in next time
	l.cacheRegions.assignedStoreIds = append(l.cacheRegions.assignedStoreIds, newPeer.GetStoreId())

	op, err := operator.CreateMovePeerOperator("balance-adjacent-peer", cluster, region, operator.OpAdjacent, leaderStoreID, newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(l.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.SetPriorityLevel(core.LowPriority)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent-peer").Inc()
	return op
}
