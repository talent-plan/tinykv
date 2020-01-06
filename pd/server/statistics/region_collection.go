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

package statistics

import (
	"github.com/pingcap-incubator/tinykv/pd/server/core"
)

// RegionStatisticType represents the type of the region's status.
type RegionStatisticType uint32

// region status type
const (
	MissPeer RegionStatisticType = 1 << iota
	ExtraPeer
	DownPeer
	PendingPeer
	OfflinePeer
	LearnerPeer
	EmptyRegion
)

const nonIsolation = "none"

// RegionStatistics is used to record the status of regions.
type RegionStatistics struct {
	opt   ScheduleOptions
	stats map[RegionStatisticType]map[uint64]*core.RegionInfo
	index map[uint64]RegionStatisticType
}

// NewRegionStatistics creates a new RegionStatistics.
func NewRegionStatistics(opt ScheduleOptions) *RegionStatistics {
	r := &RegionStatistics{
		opt:   opt,
		stats: make(map[RegionStatisticType]map[uint64]*core.RegionInfo),
		index: make(map[uint64]RegionStatisticType),
	}
	r.stats[MissPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[ExtraPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[DownPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[PendingPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[OfflinePeer] = make(map[uint64]*core.RegionInfo)
	r.stats[LearnerPeer] = make(map[uint64]*core.RegionInfo)
	r.stats[EmptyRegion] = make(map[uint64]*core.RegionInfo)
	return r
}

// GetRegionStatsByType gets the status of the region by types.
func (r *RegionStatistics) GetRegionStatsByType(typ RegionStatisticType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for _, r := range r.stats[typ] {
		res = append(res, r)
	}
	return res
}

func (r *RegionStatistics) deleteEntry(deleteIndex RegionStatisticType, regionID uint64) {
	for typ := RegionStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], regionID)
		}
	}
}

// Observe records the current regions' status.
func (r *RegionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	// Region state.
	regionID := region.GetID()
	var (
		peerTypeIndex RegionStatisticType
		deleteIndex   RegionStatisticType
	)
	if len(region.GetPeers()) < r.opt.GetMaxReplicas() {
		r.stats[MissPeer][regionID] = region
		peerTypeIndex |= MissPeer
	} else if len(region.GetPeers()) > r.opt.GetMaxReplicas() {
		r.stats[ExtraPeer][regionID] = region
		peerTypeIndex |= ExtraPeer
	}

	if len(region.GetDownPeers()) > 0 {
		r.stats[DownPeer][regionID] = region
		peerTypeIndex |= DownPeer
	}

	if len(region.GetPendingPeers()) > 0 {
		r.stats[PendingPeer][regionID] = region
		peerTypeIndex |= PendingPeer
	}

	if len(region.GetLearners()) > 0 {
		r.stats[LearnerPeer][regionID] = region
		peerTypeIndex |= LearnerPeer
	}

	if region.GetApproximateSize() <= core.EmptyRegionApproximateSize {
		r.stats[EmptyRegion][regionID] = region
		peerTypeIndex |= EmptyRegion
	}

	for _, store := range stores {
		if store.IsOffline() {
			peer := region.GetStorePeer(store.GetID())
			if peer != nil {
				r.stats[OfflinePeer][regionID] = region
				peerTypeIndex |= OfflinePeer
			}
		}
	}

	if oldIndex, ok := r.index[regionID]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, regionID)
	r.index[regionID] = peerTypeIndex
}

// ClearDefunctRegion is used to handle the overlap region.
func (r *RegionStatistics) ClearDefunctRegion(regionID uint64) {
	if oldIndex, ok := r.index[regionID]; ok {
		r.deleteEntry(oldIndex, regionID)
	}
}

// Collect collects the metrics of the regions' status.
func (r *RegionStatistics) Collect() {
	regionStatusGauge.WithLabelValues("miss-peer-region-count").Set(float64(len(r.stats[MissPeer])))
	regionStatusGauge.WithLabelValues("extra-peer-region-count").Set(float64(len(r.stats[ExtraPeer])))
	regionStatusGauge.WithLabelValues("down-peer-region-count").Set(float64(len(r.stats[DownPeer])))
	regionStatusGauge.WithLabelValues("pending-peer-region-count").Set(float64(len(r.stats[PendingPeer])))
	regionStatusGauge.WithLabelValues("offline-peer-region-count").Set(float64(len(r.stats[OfflinePeer])))
	regionStatusGauge.WithLabelValues("learner-peer-region-count").Set(float64(len(r.stats[LearnerPeer])))
	regionStatusGauge.WithLabelValues("empty-region-count").Set(float64(len(r.stats[EmptyRegion])))
}

// Reset resets the metrics of the regions' status.
func (r *RegionStatistics) Reset() {
	regionStatusGauge.Reset()
}

// LabelStatistics is the statistics of the level of labels.
type LabelStatistics struct {
	regionLabelStats map[uint64]string
	labelCounter     map[string]int
}

// NewLabelStatistics creates a new LabelStatistics.
func NewLabelStatistics() *LabelStatistics {
	return &LabelStatistics{
		regionLabelStats: make(map[uint64]string),
		labelCounter:     make(map[string]int),
	}
}

// Observe records the current label status.
func (l *LabelStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo, labels []string) {
	regionID := region.GetID()
	regionIsolation := getRegionLabelIsolation(stores, labels)
	if label, ok := l.regionLabelStats[regionID]; ok {
		if label == regionIsolation {
			return
		}
		l.counterDec(label)
	}
	l.regionLabelStats[regionID] = regionIsolation
	l.counterInc(regionIsolation)
}

// Collect collects the metrics of the label status.
func (l *LabelStatistics) Collect() {
	for level, count := range l.labelCounter {
		regionLabelLevelGauge.WithLabelValues(level).Set(float64(count))
	}
}

// Reset resets the metrics of the label status.
func (l *LabelStatistics) Reset() {
	regionLabelLevelGauge.Reset()
}

// ClearDefunctRegion is used to handle the overlap region.
func (l *LabelStatistics) ClearDefunctRegion(regionID uint64, labels []string) {
	if label, ok := l.regionLabelStats[regionID]; ok {
		l.counterDec(label)
		delete(l.regionLabelStats, regionID)
	}
}

func (l *LabelStatistics) counterInc(label string) {
	if label == nonIsolation {
		l.labelCounter[nonIsolation]++
	} else {
		l.labelCounter[label]++
	}
}

func (l *LabelStatistics) counterDec(label string) {
	if label == nonIsolation {
		l.labelCounter[nonIsolation]--
	} else {
		l.labelCounter[label]--
	}
}

func getRegionLabelIsolation(stores []*core.StoreInfo, labels []string) string {
	if len(stores) == 0 || len(labels) == 0 {
		return nonIsolation
	}
	queueStores := [][]*core.StoreInfo{stores}
	for level, label := range labels {
		newQueueStores := make([][]*core.StoreInfo, 0, len(stores))
		for _, stores := range queueStores {
			notIsolatedStores := notIsolatedStoresWithLabel(stores, label)
			if len(notIsolatedStores) > 0 {
				newQueueStores = append(newQueueStores, notIsolatedStores...)
			}
		}
		queueStores = newQueueStores
		if len(queueStores) == 0 {
			return labels[level]
		}
	}
	return nonIsolation
}

func notIsolatedStoresWithLabel(stores []*core.StoreInfo, label string) [][]*core.StoreInfo {
	m := make(map[string][]*core.StoreInfo)
	for _, s := range stores {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			continue
		}
		m[labelValue] = append(m[labelValue], s)
	}
	var res [][]*core.StoreInfo
	for _, stores := range m {
		if len(stores) > 1 {
			res = append(res, stores)
		}
	}
	return res
}
