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

	c.Assert(compareStoreScore(store1, store2), Equals, 0)

	c.Assert(compareStoreScore(store1, store3), Equals, 1)
}
