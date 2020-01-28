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
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
)

// options for interval of schedulers
const (
	MaxScheduleInterval     = time.Second * 5
	MinScheduleInterval     = time.Millisecond * 10
	MinSlowScheduleInterval = time.Second * 3

	ScheduleIntervalFactor = 1.3
)

type intervalGrowthType int

const (
	exponentailGrowth intervalGrowthType = iota
	linearGrowth
	zeroGrowth
)

// intervalGrow calculates the next interval of balance.
func intervalGrow(x time.Duration, maxInterval time.Duration, typ intervalGrowthType) time.Duration {
	switch typ {
	case exponentailGrowth:
		return minDuration(time.Duration(float64(x)*ScheduleIntervalFactor), maxInterval)
	case linearGrowth:
		return minDuration(x+MinSlowScheduleInterval, maxInterval)
	case zeroGrowth:
		return x
	default:
		log.Fatal("unknown interval growth type")
	}
	return 0
}

type baseScheduler struct {
	opController *schedule.OperatorController
}

func newBaseScheduler(opController *schedule.OperatorController) *baseScheduler {
	return &baseScheduler{opController: opController}
}

func (s *baseScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implements")
}

func (s *baseScheduler) GetMinInterval() time.Duration {
	return MinScheduleInterval
}

func (s *baseScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(nil)
}

func (s *baseScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, MaxScheduleInterval, exponentailGrowth)
}

func (s *baseScheduler) Prepare(cluster opt.Cluster) error { return nil }

func (s *baseScheduler) Cleanup(cluster opt.Cluster) {}
