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

package schedule

import "github.com/prometheus/client_golang/prometheus"

var (
	operatorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operators_count",
			Help:      "Counter of schedule operators.",
		}, []string{"type", "event"})

	operatorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "finish_operators_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})

	operatorWaitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "operators_waiting_count",
			Help:      "Counter of schedule waiting operators.",
		}, []string{"type", "event"})

	operatorWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "waiting_operators_duration_seconds",
			Help:      "Bucketed histogram of waiting time (s) of operator for being promoted.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})

	storeLimitGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "store_limit",
			Help:      "Limit of store.",
		}, []string{"store", "type"})
)

func init() {
	prometheus.MustRegister(operatorCounter)
	prometheus.MustRegister(operatorDuration)
	prometheus.MustRegister(operatorWaitDuration)
	prometheus.MustRegister(storeLimitGauge)
	prometheus.MustRegister(operatorWaitCounter)
}
