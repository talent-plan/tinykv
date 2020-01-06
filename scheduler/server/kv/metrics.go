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

package kv

import "github.com/prometheus/client_golang/prometheus"

var (
	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "txn",
			Name:      "txns_count",
			Help:      "Counter of txns.",
		}, []string{"result"})

	txnDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "txn",
			Name:      "handle_txns_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled txns.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"result"})
)

func init() {
	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(txnDuration)
}
