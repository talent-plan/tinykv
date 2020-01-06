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

package filter

import "github.com/prometheus/client_golang/prometheus"

var (
	filterCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "filter",
			Help:      "Counter of the filter",
		}, []string{"action", "address", "store", "scope", "type"})
)

func init() {
	prometheus.MustRegister(filterCounter)
}
