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

package server

import "github.com/prometheus/client_golang/prometheus"

var (
	healthStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "health_status",
			Help:      "Status of the cluster.",
		}, []string{"name"})

	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})

	schedulerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "status",
			Help:      "Status of the scheduler.",
		}, []string{"kind", "type"})

	regionHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat",
			Help:      "Counter of region hearbeat.",
		}, []string{"address", "store", "type", "status"})

	regionEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "region_event",
			Help:      "Counter of the region event",
		}, []string{"event"})

	regionHeartbeatLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat_latency_seconds",
			Help:      "Bucketed histogram of latency (s) of receiving heartbeat.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{"address", "store"})

	hotSpotStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "hotspot",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"address", "store", "type"})

	metadataGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "metadata",
			Help:      "Record critical metadata.",
		}, []string{"type"})

	etcdStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "etcd_state",
			Help:      "Etcd raft states.",
		}, []string{"type"})

	patrolCheckRegionsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "patrol",
			Name:      "checks_regions",
			Help:      "Bucketed histogram of time spend(s) of patrol checks region.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		})

	tsoHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
)

func init() {
	prometheus.MustRegister(timeJumpBackCounter)
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(regionHeartbeatCounter)
	prometheus.MustRegister(regionEventCounter)
	prometheus.MustRegister(regionHeartbeatLatency)
	prometheus.MustRegister(hotSpotStatusGauge)
	prometheus.MustRegister(metadataGauge)
	prometheus.MustRegister(etcdStateGauge)
	prometheus.MustRegister(patrolCheckRegionsHistogram)
	prometheus.MustRegister(tsoHandleDuration)
}
