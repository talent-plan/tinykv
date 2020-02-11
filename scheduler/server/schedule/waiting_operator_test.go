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
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	. "github.com/pingcap/check"
)

var _ = Suite(&testWaitingOperatorSuite{})

type testWaitingOperatorSuite struct{}

func (s *testWaitingOperatorSuite) TestRandBuckets(c *C) {
	rb := NewRandBuckets()
	addOperators(rb)
	for i := 0; i < 3; i++ {
		op := rb.GetOperator()
		c.Assert(op, NotNil)
	}
	c.Assert(rb.GetOperator(), IsNil)
}

func addOperators(wop WaitingOperator) {
	op := operator.NewOperator("testOperatorNormal", "test", uint64(1), &metapb.RegionEpoch{}, operator.OpRegion, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(1)},
	}...)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorHigh", "test", uint64(2), &metapb.RegionEpoch{}, operator.OpRegion, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(2)},
	}...)
	op.SetPriorityLevel(core.HighPriority)
	wop.PutOperator(op)
	op = operator.NewOperator("testOperatorLow", "test", uint64(3), &metapb.RegionEpoch{}, operator.OpRegion, []operator.OpStep{
		operator.RemovePeer{FromStore: uint64(3)},
	}...)
	op.SetPriorityLevel(core.LowPriority)
	wop.PutOperator(op)
}

func (s *testWaitingOperatorSuite) TestListOperator(c *C) {
	rb := NewRandBuckets()
	addOperators(rb)
	c.Assert(len(rb.ListOperator()), Equals, 3)
}
