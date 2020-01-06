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

package checker

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
)

// LearnerChecker ensures region has a learner will be promoted.
type LearnerChecker struct{}

// NewLearnerChecker creates a learner checker.
func NewLearnerChecker() *LearnerChecker { return &LearnerChecker{} }

// Check verifies a region's role, creating an Operator if need.
func (l *LearnerChecker) Check(region *core.RegionInfo) *operator.Operator {
	for _, p := range region.GetLearners() {
		if region.GetPendingLearner(p.GetId()) != nil {
			continue
		}
		op := operator.CreatePromoteLearnerOperator("promote-learner", region, p)
		return op
	}
	return nil
}
