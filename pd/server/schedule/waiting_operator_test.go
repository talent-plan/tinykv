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
	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
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

func (s *testWaitingOperatorSuite) TestRandomBucketsWithMergeRegion(c *C) {
	rb := NewRandBuckets()
	descs := []string{"merge-region", "admin-merge-region", "random-merge"}
	for j := 0; j < 100; j++ {
		// adds operators
		desc := descs[j%3]
		op := operator.NewOperator(desc, "test", uint64(1), &metapb.RegionEpoch{}, operator.OpRegion|operator.OpMerge, []operator.OpStep{
			operator.MergeRegion{
				FromRegion: &metapb.Region{
					Id:          1,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				ToRegion: &metapb.Region{Id: 2,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				IsPassive: false,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator(desc, "test", uint64(2), &metapb.RegionEpoch{}, operator.OpRegion|operator.OpMerge, []operator.OpStep{
			operator.MergeRegion{
				FromRegion: &metapb.Region{
					Id:          1,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				ToRegion: &metapb.Region{Id: 2,
					StartKey:    []byte{},
					EndKey:      []byte{},
					RegionEpoch: &metapb.RegionEpoch{}},
				IsPassive: true,
			},
		}...)
		rb.PutOperator(op)
		op = operator.NewOperator("testOperatorHigh", "test", uint64(3), &metapb.RegionEpoch{}, operator.OpRegion, []operator.OpStep{
			operator.RemovePeer{FromStore: uint64(3)},
		}...)
		op.SetPriorityLevel(core.HighPriority)
		rb.PutOperator(op)

		for i := 0; i < 2; i++ {
			op := rb.GetOperator()
			c.Assert(op, NotNil)
		}
		c.Assert(rb.GetOperator(), IsNil)
	}
}
