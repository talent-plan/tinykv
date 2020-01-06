// Copyright 2019 PingCAP, Inc.
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

import "github.com/prometheus/client_golang/prometheus"

var (
	hotCacheStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "hotcache",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"name", "store", "type"})

	storeStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_status",
			Help:      "Store status for schedule",
		}, []string{"address", "store", "type"})

	regionStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "regions",
			Name:      "status",
			Help:      "Status of the regions.",
		}, []string{"type"})

	clusterStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "status",
			Help:      "Status of the cluster.",
		}, []string{"type"})

	placementStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "placement_status",
			Help:      "Status of the cluster placement.",
		}, []string{"type", "name"})

	configStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "config",
			Name:      "status",
			Help:      "Status of the scheduling configurations.",
		}, []string{"type"})

	regionLabelLevelGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "regions",
			Name:      "label_level",
			Help:      "Number of regions in the different label level.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(hotCacheStatusGauge)
	prometheus.MustRegister(storeStatusGauge)
	prometheus.MustRegister(regionStatusGauge)
	prometheus.MustRegister(clusterStatusGauge)
	prometheus.MustRegister(placementStatusGauge)
	prometheus.MustRegister(configStatusGauge)
	prometheus.MustRegister(regionLabelLevelGauge)
}
