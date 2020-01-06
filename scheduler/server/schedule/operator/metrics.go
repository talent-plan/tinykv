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

package operator

import "github.com/prometheus/client_golang/prometheus"

var (
	operatorStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "finish_operator_steps_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator step.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(operatorStepDuration)
}
