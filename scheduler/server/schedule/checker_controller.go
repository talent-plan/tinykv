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

package schedule

import (
	"context"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/checker"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster        opt.Cluster
	opController   *OperatorController
	learnerChecker *checker.LearnerChecker
	replicaChecker *checker.ReplicaChecker
	mergeChecker   *checker.MergeChecker
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(ctx context.Context, cluster opt.Cluster, opController *OperatorController) *CheckerController {
	return &CheckerController{
		cluster:        cluster,
		opController:   opController,
		learnerChecker: checker.NewLearnerChecker(),
		replicaChecker: checker.NewReplicaChecker(cluster),
		mergeChecker:   checker.NewMergeChecker(ctx, cluster),
	}
}

// CheckRegion will check the region and add a new operator if needed.
func (c *CheckerController) CheckRegion(region *core.RegionInfo) (bool, []*operator.Operator) { //return checkerIsBusy,ops
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController
	checkerIsBusy := true
	if op := c.learnerChecker.Check(region); op != nil {
		return false, []*operator.Operator{op}
	}
	if opController.OperatorCount(operator.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		checkerIsBusy = false
		if op := c.replicaChecker.Check(region); op != nil {
			return checkerIsBusy, []*operator.Operator{op}
		}
	}
	if c.mergeChecker != nil && opController.OperatorCount(operator.OpMerge) < c.cluster.GetMergeScheduleLimit() {
		checkerIsBusy = false
		if ops := c.mergeChecker.Check(region); ops != nil {
			// It makes sure that two operators can be added successfully altogether.
			return checkerIsBusy, ops
		}
	}
	return checkerIsBusy, nil
}

// GetMergeChecker returns the merge checker.
func (c *CheckerController) GetMergeChecker() *checker.MergeChecker {
	return c.mergeChecker
}
