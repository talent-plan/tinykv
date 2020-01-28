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

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFiltersSuite{})

type testFiltersSuite struct{}

func (s *testFiltersSuite) TestPendingPeerFilter(c *C) {
	filter := NewPendingPeerCountFilter("")
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	store := core.NewStoreInfo(&metapb.Store{Id: 1})
	c.Assert(filter.Source(tc, store), IsFalse)
	newStore := store.Clone(core.SetPendingPeerCount(30))
	c.Assert(filter.Source(tc, newStore), IsTrue)
	c.Assert(filter.Target(tc, newStore), IsTrue)
	// set to 0 means no limit
	opt.MaxPendingPeerCount = 0
	c.Assert(filter.Source(tc, newStore), IsFalse)
	c.Assert(filter.Target(tc, newStore), IsFalse)
}
