// Copyright 2018 PingCAP, Inc.
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
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/pd/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("shuffle-hot-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return err
				}
				conf.Limit = limit
			}
			return nil
		}
	})

	schedule.RegisterScheduler("shuffle-hot-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		decoder(conf)
		return newShuffleHotRegionScheduler(opController, conf), nil
	})
}

type shuffleHotRegionSchedulerConfig struct {
	Limit uint64 `json:"limit"`
}

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*baseScheduler
	stats *storeStatistics
	r     *rand.Rand
	conf  *shuffleHotRegionSchedulerConfig
	types []BalanceType
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *schedule.OperatorController, conf *shuffleHotRegionSchedulerConfig) schedule.Scheduler {
	base := newBaseScheduler(opController)
	return &shuffleHotRegionScheduler{
		baseScheduler: base,
		conf:          conf,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotReadRegionBalance, hotWriteRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return "shuffle-hot-region-scheduler"
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return "shuffle-hot-region"
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpHotRegion) < s.conf.Limit &&
		s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit() &&
		s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *shuffleHotRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	i := s.r.Int() % len(s.types)
	return s.dispatch(s.types[i], cluster)
}

func (s *shuffleHotRegionScheduler) dispatch(typ BalanceType, cluster opt.Cluster) []*operator.Operator {
	switch typ {
	case hotReadRegionBalance:
		s.stats.readStatAsLeader = calcScore(cluster.RegionReadStats(), cluster, core.LeaderKind)
		return s.randomSchedule(cluster, s.stats.readStatAsLeader)
	case hotWriteRegionBalance:
		s.stats.writeStatAsLeader = calcScore(cluster.RegionWriteStats(), cluster, core.LeaderKind)
		return s.randomSchedule(cluster, s.stats.writeStatAsLeader)
	}
	return nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster opt.Cluster, storeStats statistics.StoreHotRegionsStat) []*operator.Operator {
	for _, stats := range storeStats {
		i := s.r.Intn(len(stats.RegionsStat))
		r := stats.RegionsStat[i]
		// select src region
		srcRegion := cluster.GetRegion(r.RegionID)
		if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcRegion.GetLeader().GetStoreId()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID))
		}
		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore),
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filter.Target(cluster, store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest store
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			return nil
		}
		destPeer, err := cluster.AllocPeer(destStoreID)
		if err != nil {
			log.Error("failed to allocate peer", zap.Error(err))
			return nil
		}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStoreID, destStoreID, destPeer.GetId())
		if err != nil {
			return nil
		}
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator").Inc()
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}
