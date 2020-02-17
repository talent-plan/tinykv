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

package selector

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSelectorSuite{})

type testSelectorSuite struct {
	tc *mockcluster.Cluster
}

func (s *testSelectorSuite) SetUpSuite(c *C) {
	opt := mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(opt)
}

func (s *testSelectorSuite) TestCompareStoreScore(c *C) {
	store1 := core.NewStoreInfoWithIdAndCount(1, 1)
	store2 := core.NewStoreInfoWithIdAndCount(2, 1)
	store3 := core.NewStoreInfoWithIdAndCount(3, 3)

	c.Assert(compareStoreScore(s.tc, store1, store2), Equals, 0)

	c.Assert(compareStoreScore(s.tc, store1, store3), Equals, 1)
}

func (s *testSelectorSuite) TestScheduleConfig(c *C) {
	filters := make([]filter.Filter, 0)
	testScheduleConfig := func(selector *BalanceSelector, stores []*core.StoreInfo, expectSourceID, expectTargetID uint64) {
		c.Assert(selector.SelectSource(s.tc, stores).GetID(), Equals, expectSourceID)
		c.Assert(selector.SelectTarget(s.tc, stores).GetID(), Equals, expectTargetID)
	}

	kinds := []core.ScheduleKind{{
		Resource: core.RegionKind,
		Strategy: core.ByCount,
	}, {
		Resource: core.RegionKind,
		Strategy: core.BySize,
	}}

	for _, kind := range kinds {
		selector := NewBalanceSelector(kind, filters)
		stores := []*core.StoreInfo{
			core.NewStoreInfoWithSizeCount(1, 2, 3, 10, 5),
			core.NewStoreInfoWithSizeCount(2, 2, 3, 4, 5),
			core.NewStoreInfoWithSizeCount(3, 2, 3, 4, 5),
			core.NewStoreInfoWithSizeCount(4, 2, 3, 2, 5),
		}
		testScheduleConfig(selector, stores, 1, 4)
	}

	selector := NewBalanceSelector(core.ScheduleKind{
		Resource: core.LeaderKind,
		Strategy: core.ByCount,
	}, filters)
	stores := []*core.StoreInfo{
		core.NewStoreInfoWithSizeCount(1, 2, 20, 10, 25),
		core.NewStoreInfoWithSizeCount(2, 2, 66, 10, 5),
		core.NewStoreInfoWithSizeCount(3, 2, 6, 10, 5),
		core.NewStoreInfoWithSizeCount(4, 2, 20, 10, 1),
	}
	testScheduleConfig(selector, stores, 2, 3)
	s.tc.LeaderScheduleStrategy = core.BySize.String()
	testScheduleConfig(selector, stores, 1, 4)
}
