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

package tso

import "github.com/prometheus/client_golang/prometheus"

var (
	tsoCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "tso",
			Name:      "events",
			Help:      "Counter of tso events",
		}, []string{"type"})

	tsoGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "tso",
			Help:      "Record of tso metadata.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(tsoCounter)
	prometheus.MustRegister(tsoGauge)
}
