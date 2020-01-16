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
	"context"
	"math"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/checker"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	. "github.com/pingcap/check"
)

func newTestReplication(mso *mockoption.ScheduleOptions, maxReplicas int, locationLabels ...string) {
	mso.MaxReplicas = maxReplicas
	mso.LocationLabels = locationLabels
}

var _ = Suite(&testBalanceSpeedSuite{})

type testBalanceSpeedSuite struct{}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	regionSize     int64
	expectedResult bool
	kind           core.ScheduleStrategy
}

func (s *testBalanceSpeedSuite) TestShouldBalance(c *C) {
	tests := []testBalanceSpeedCase{
		// all store capacity is 1024MB
		// size = count * 10

		// target size is zero
		{2, 0, 1, true, core.BySize},
		{2, 0, 10, false, core.BySize},
		// all in high space stage
		{10, 5, 1, true, core.BySize},
		{10, 5, 20, false, core.BySize},
		{10, 10, 1, false, core.BySize},
		{10, 10, 20, false, core.BySize},
		// all in transition stage
		{70, 50, 1, true, core.BySize},
		{70, 50, 50, false, core.BySize},
		{70, 70, 1, false, core.BySize},
		// all in low space stage
		{90, 80, 1, true, core.BySize},
		{90, 80, 50, false, core.BySize},
		{90, 90, 1, false, core.BySize},
		// one in high space stage, other in transition stage
		{65, 55, 5, true, core.BySize},
		{65, 50, 50, false, core.BySize},
		// one in transition space stage, other in low space stage
		{80, 70, 5, true, core.BySize},
		{80, 70, 50, false, core.BySize},

		// default leader tolerant ratio is 5, when schedule by count
		// target size is zero
		{2, 0, 1, false, core.ByCount},
		{2, 0, 10, false, core.ByCount},
		// all in high space stage
		{10, 5, 1, true, core.ByCount},
		{10, 5, 20, true, core.ByCount},
		{10, 6, 20, false, core.ByCount},
		{10, 10, 1, false, core.ByCount},
		{10, 10, 20, false, core.ByCount},
		// all in transition stage
		{70, 50, 1, true, core.ByCount},
		{70, 50, 50, true, core.ByCount},
		{70, 70, 1, false, core.ByCount},
		// all in low space stage
		{90, 80, 1, true, core.ByCount},
		{90, 80, 50, true, core.ByCount},
		{90, 90, 1, false, core.ByCount},
		// one in high space stage, other in transition stage
		{65, 55, 5, true, core.ByCount},
		{65, 50, 50, true, core.ByCount},
		// one in transition space stage, other in low space stage
		{80, 70, 5, true, core.ByCount},
		{80, 70, 50, true, core.ByCount},
	}

	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	// create a region to control average region size.
	tc.AddLeaderRegion(1, 1, 2)

	for _, t := range tests {
		tc.AddLeaderStore(1, int(t.sourceCount))
		tc.AddLeaderStore(2, int(t.targetCount))
		source := tc.GetStore(1)
		target := tc.GetStore(2)
		region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
		tc.PutRegion(region)
		tc.LeaderScheduleStrategy = t.kind.String()
		kind := core.NewScheduleKind(core.LeaderKind, t.kind)
		c.Assert(shouldBalance(tc, source, target, region, kind, schedule.NewUnfinishedOpInfluence(nil, tc), ""), Equals, t.expectedResult)
	}

	for _, t := range tests {
		if t.kind.String() == core.BySize.String() {
			tc.AddRegionStore(1, int(t.sourceCount))
			tc.AddRegionStore(2, int(t.targetCount))
			source := tc.GetStore(1)
			target := tc.GetStore(2)
			region := tc.GetRegion(1).Clone(core.SetApproximateSize(t.regionSize))
			tc.PutRegion(region)
			kind := core.NewScheduleKind(core.RegionKind, t.kind)
			c.Assert(shouldBalance(tc, source, target, region, kind, schedule.NewUnfinishedOpInfluence(nil, tc), ""), Equals, t.expectedResult)
		}
	}
}

func (s *testBalanceSpeedSuite) TestBalanceLimit(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddLeaderStore(1, 10)
	tc.AddLeaderStore(2, 20)
	tc.AddLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(200.0/3.0)))

	tc.SetStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(50.0/2.0)))
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
}

func (s *testBalanceLeaderSchedulerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	opt := mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
	lb, err := schedule.CreateScheduler("balance-leader", s.oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testBalanceLeaderSchedulerSuite) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLimit(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 0)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    16   0    0    0
	// Region1:    L    F    F    F
	s.tc.UpdateLeaderCount(1, 16)
	c.Check(s.schedule(), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 7)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 10)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(4, 16)
	c.Check(s.schedule(), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLeaderScheduleStrategy(c *C) {
	// Stores:			1    	2    	3    	4
	// Leader Count:		10    	10    	10    	10
	// Leader Size :		10000   100    	100    	100
	// Region1:			L    	F   	F    	F
	s.tc.AddLeaderStore(1, 10, 10000)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Assert(s.tc.LeaderScheduleStrategy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), IsNil)
	s.tc.LeaderScheduleStrategy = core.BySize.String()
	c.Check(s.schedule(), NotNil)
}

func (s *testBalanceSpeedSuite) TestTolerantRatio(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	// create a region to control average region size.
	tc.AddLeaderRegion(1, 1, 2)
	regionSize := int64(96 * 1024)
	region := tc.GetRegion(1).Clone(core.SetApproximateSize(regionSize))

	tc.TolerantSizeRatio = 0
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Strategy: core.ByCount}), Equals, int64(leaderTolerantSizeRatio))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Strategy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Strategy: core.ByCount}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Strategy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))

	tc.TolerantSizeRatio = 10
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Strategy: core.ByCount}), Equals, int64(tc.TolerantSizeRatio))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.LeaderKind, Strategy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Strategy: core.ByCount}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
	c.Assert(getTolerantResource(tc, region, core.ScheduleKind{Resource: core.RegionKind, Strategy: core.BySize}), Equals, int64(adjustTolerantRatio(tc)*float64(regionSize)))
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLeaderTolerantRatio(c *C) {
	// default leader tolerant ratio is 5, when schedule by count
	// Stores:			1		2    	3    	4
	// Leader Count:		14->15		10    	10    	10
	// Leader Size :		100		100    	100    	100
	// Region1:			L		F   	F    	F
	s.tc.AddLeaderStore(1, 14, 100)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Assert(s.tc.LeaderScheduleStrategy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), IsNil)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 14)
	s.tc.AddLeaderStore(1, 15, 100)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 15)
	c.Check(s.schedule(), NotNil)
	s.tc.TolerantSizeRatio = 6 // (15-10)<6
	c.Check(s.schedule(), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestScheduleWithOpInfluence(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   14
	// Region1:    F    F    F    L
	s.tc.AddLeaderStore(1, 7)
	s.tc.AddLeaderStore(2, 8)
	s.tc.AddLeaderStore(3, 9)
	s.tc.AddLeaderStore(4, 14)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	op := s.schedule()[0]
	c.Check(op, NotNil)
	s.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of store1 and store4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader difference is not greater than 5.
	c.Assert(s.tc.LeaderScheduleStrategy, Equals, core.ByCount.String()) // default by count
	c.Check(s.schedule(), NotNil)
	s.tc.LeaderScheduleStrategy = core.BySize.String()
	c.Check(s.schedule(), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   13
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 8)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 13)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceFilter(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderRegion(1, 4, 1, 2, 3)

	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we should consider it
	// because it still provides services
	s.tc.SetStoreOffline(4)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.SetStoreDown(1)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.SetStoreBusy(2, true)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 3)

	// Test disconnectFilter.
	// If store 3 is disconnected, no operator can be created.
	s.tc.SetStoreDisconnect(3)
	c.Assert(s.schedule(), HasLen, 0)
}

func (s *testBalanceLeaderSchedulerSuite) TestLeaderWeight(c *C) {
	// Stores:	1	2	3	4
	// Leaders:    10      10      10      10
	// Weight:    0.5     0.9       1       2
	// Region1:     L       F       F       F

	s.tc.AddLeaderStore(1, 10)
	s.tc.AddLeaderStore(2, 10)
	s.tc.AddLeaderStore(3, 10)
	s.tc.AddLeaderStore(4, 10)
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 1, 4)
	s.tc.UpdateLeaderCount(4, 30)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 1, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceSelector(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderRegion(1, 4, 2, 3)
	s.tc.AddLeaderRegion(2, 3, 1, 2)
	// store4 has max leader score, store1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.UpdateLeaderCount(2, 14)
	s.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderRegion(1, 3, 2, 4)
	s.tc.AddLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. Now source and target are store3 and store2.
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 3, 2)

	// Stores:     1    2    3    4
	// Leaders:    9    10   10   11
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(1, 10)
	s.tc.AddLeaderStore(2, 10)
	s.tc.AddLeaderStore(3, 10)
	s.tc.AddLeaderStore(4, 10)
	s.tc.AddLeaderRegion(1, 4, 2, 3)
	s.tc.AddLeaderRegion(2, 1, 2, 3)
	// The cluster is balanced.
	c.Assert(s.schedule(), IsNil)
	c.Assert(s.schedule(), IsNil)

	// store3's leader drops:
	// Stores:     1    2    3    4
	// Leaders:    11   13   0    16
	// Region1:    -    F    F    L
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(1, 11)
	s.tc.AddLeaderStore(2, 13)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 16)
	testutil.CheckTransferLeader(c, s.schedule()[0], operator.OpBalance, 4, 3)
}

var _ = Suite(&testBalanceRegionSchedulerSuite{})

type testBalanceRegionSchedulerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testBalanceRegionSchedulerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testBalanceRegionSchedulerSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testBalanceRegionSchedulerSuite) TestBalance(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	opt.SetMaxReplicas(1)

	// Add stores 1,2,3,4.
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 8)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 16)
	// Add region 1 with leader in store 4.
	tc.AddLeaderRegion(1, 4)
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpBalance, 4, 1)

	// Test stateFilter.
	tc.SetStoreOffline(1)
	tc.UpdateRegionCount(2, 6)

	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc)[0], operator.OpBalance, 4, 2)
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(tc), IsNil)

	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(tc), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 3, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores.
	c.Assert(sb.Schedule(tc), IsNil)

	// Store 4 has smaller region score than store 2.
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.AddLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 6)

	// Store 7 has smaller region score with store 6.
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 7)

	// If store 7 is not available, will choose store 6.
	tc.SetStoreDown(7)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 6)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 6)

	// Take down 4,5,6,7
	tc.SetStoreDown(4)
	tc.SetStoreDown(5)
	tc.SetStoreDown(6)
	tc.SetStoreDown(7)
	tc.SetStoreDown(8)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.AddLabelsStore(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	c.Assert(sb.Schedule(tc), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 5, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc.AddLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 5, 6)

	// Store 7 has larger region score and same distinct score with store 6.
	tc.AddLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 5, 6)

	// Store 1 has smaller region score and higher distinct score.
	tc.AddLeaderRegion(1, 2, 3, 4, 5, 6)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.AddLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.AddLeaderRegion(1, 2, 3, 11, 12, 13)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 11, 6)
}

// TestBalance2 for cornor case 1:
// 11 regions distributed across 5 stores.
//| region_id | leader_store | follower_store | follower_store |
//|-----------|--------------|----------------|----------------|
//|     1     |       1      |        2       |       3        |
//|     2     |       1      |        2       |       3        |
//|     3     |       1      |        2       |       3        |
//|     4     |       1      |        2       |       3        |
//|     5     |       1      |        2       |       3        |
//|     6     |       1      |        2       |       3        |
//|     7     |       1      |        2       |       4        |
//|     8     |       1      |        2       |       4        |
//|     9     |       1      |        2       |       4        |
//|    10     |       1      |        4       |       5        |
//|    11     |       1      |        4       |       5        |
// and the space of last store 5 if very small, about 5 * regionsize
// the source region is more likely distributed in store[1, 2, 3].
func (s *testBalanceRegionSchedulerSuite) TestBalance1(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	opt.TolerantSizeRatio = 1

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 11)
	tc.AddRegionStore(2, 9)
	tc.AddRegionStore(3, 6)
	tc.AddRegionStore(4, 5)
	tc.AddRegionStore(5, 2)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)

	c.Assert(sb.Schedule(tc)[0], NotNil)
	// if the space of store 5 is normal, we can balance region to store 5
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 5)

	// the used size of  store 5 reach (highSpace, lowSpace)
	origin := tc.GetStore(5)
	stats := origin.GetStoreStats()
	stats.Capacity = 50
	stats.Available = 28
	stats.UsedSize = 20
	store5 := origin.Clone(core.SetStoreStats(stats))
	tc.PutStore(store5)

	// the scheduler first picks store 1 as source store,
	// and store 5 as target store, but cannot pass `shouldBalance`.
	// Then it will try store4.
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 4)
}

func (s *testBalanceRegionSchedulerSuite) TestStoreWeight(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)

	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)
	tc.AddRegionStore(4, 10)
	tc.UpdateStoreRegionWeight(1, 0.5)
	tc.UpdateStoreRegionWeight(2, 0.9)
	tc.UpdateStoreRegionWeight(3, 1.0)
	tc.UpdateStoreRegionWeight(4, 2.0)

	tc.AddLeaderRegion(1, 1)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 4)

	tc.UpdateRegionCount(4, 30)
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 3)
}

func (s *testBalanceRegionSchedulerSuite) TestReplacePendingRegion(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 3, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 7, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})
	// Store 4 has smaller region score than store 1 and more better place than store 2.
	tc.AddLabelsStore(4, 10, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// set pending peer
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 2, 1, 3)
	region := tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(1)}))
	tc.PutRegion(region)

	c.Assert(sb.Schedule(tc)[0].RegionID(), Equals, uint64(3))
	testutil.CheckTransferPeer(c, sb.Schedule(tc)[0], operator.OpBalance, 1, 4)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) TestBasic(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	rc := checker.NewReplicaChecker(tc)

	opt.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.AddRegionStore(1, 4)
	tc.AddRegionStore(2, 3)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.AddLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := tc.GetRegion(1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Disable make up replica feature.
	opt.EnableMakeUpReplica = false
	c.Assert(rc.Check(region), IsNil)
	opt.EnableMakeUpReplica = true

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.SetStoreDown(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	tc.SetStoreUp(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to store 3.
	tc.UpdateSnapshotCount(4, 3)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.UpdateSnapshotCount(4, 1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	region = region.Clone(core.WithAddPeer(peer4))
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	region = region.Clone(core.WithAddPeer(peer3))
	testutil.CheckRemovePeer(c, rc.Check(region), 1)

	// Disable remove extra replica feature.
	opt.EnableRemoveExtraReplica = false
	c.Assert(rc.Check(region), IsNil)
	opt.EnableRemoveExtraReplica = true

	region = region.Clone(core.WithRemoveStorePeer(1))

	// Peer in store 2 is down, remove it.
	tc.SetStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}

	region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 1)
	region = region.Clone(core.WithDownPeers(nil))
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 3 is offline, transfer peer to store 1.
	tc.SetStoreOffline(3)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 1)
}

func (s *testReplicaCheckerSuite) TestLostStore(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)

	rc := checker.NewReplicaChecker(tc)

	// now region peer in store 1,2,3.but we just have store 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)
	op := rc.Check(region)
	c.Assert(op, IsNil)
}

func (s *testReplicaCheckerSuite) TestOffline(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := checker.NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1)

	// Store 2 has different zone and smallest region score.
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region = region.Clone(core.WithAddPeer(peer2))

	// Store 3 has different zone and smallest region score.
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	peer3, _ := tc.AllocPeer(3)
	region = region.Clone(core.WithAddPeer(peer3))

	// Store 4 has the same zone with store 3 and larger region score.
	peer4, _ := tc.AllocPeer(4)
	region = region.Clone(core.WithAddPeer(peer4))
	testutil.CheckRemovePeer(c, rc.Check(region), 4)

	// Test healthFilter.
	tc.SetStoreBusy(4, true)
	c.Assert(rc.Check(region), IsNil)
	tc.SetStoreBusy(4, false)
	testutil.CheckRemovePeer(c, rc.Check(region), 4)

	// Test offline
	// the number of region peers more than the maxReplicas
	// remove the peer
	tc.SetStoreOffline(3)
	testutil.CheckRemovePeer(c, rc.Check(region), 3)
	region = region.Clone(core.WithRemoveStorePeer(4))
	// the number of region peers equals the maxReplicas
	// Transfer peer to store 4.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 4)

	// Store 5 has a same label score with store 4,but the region score smaller than store 4, we will choose store 5.
	tc.AddLabelsStore(5, 3, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 5)
	// Store 5 has too many snapshots, choose store 4
	tc.UpdateSnapshotCount(5, 10)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 4)
	tc.UpdatePendingPeerCount(4, 30)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := checker.NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region = region.Clone(core.WithAddPeer(peer2))

	// Store 1,2,3 have the same zone, rack, and host.
	tc.AddLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)

	// Store 4 has smaller region score.
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Store 5 has a different host.
	tc.AddLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 5)

	// Store 6 has a different rack.
	tc.AddLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)

	// Store 7 has a different zone.
	tc.AddLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 7)

	// Test stateFilter.
	tc.SetStoreOffline(7)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)
	tc.SetStoreUp(7)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 7)

	// Add peer to store 7.
	peer7, _ := tc.AllocPeer(7)
	region = region.Clone(core.WithAddPeer(peer7))

	// Replace peer in store 1 with store 6 because it has a different rack.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 6)
	// Disable locationReplacement feature.
	opt.EnableLocationReplacement = false
	c.Assert(rc.Check(region), IsNil)
	opt.EnableLocationReplacement = true
	peer6, _ := tc.AllocPeer(6)
	region = region.Clone(core.WithAddPeer(peer6))
	testutil.CheckRemovePeer(c, rc.Check(region), 1)
	region = region.Clone(core.WithRemoveStorePeer(1))
	c.Assert(rc.Check(region), IsNil)

	// Store 8 has the same zone and different rack with store 7.
	// Store 1 has the same zone and different rack with store 6.
	// So store 8 and store 1 are equivalent.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	c.Assert(rc.Check(region), IsNil)

	// Store 10 has a different zone.
	// Store 2 and 6 have the same distinct score, but store 2 has larger region score.
	// So replace peer in store 2 with store 10.
	tc.AddLabelsStore(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 10)
	peer10, _ := tc.AllocPeer(10)
	region = region.Clone(core.WithAddPeer(peer10))
	testutil.CheckRemovePeer(c, rc.Check(region), 2)
	region = region.Clone(core.WithRemoveStorePeer(2))
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore2(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	newTestReplication(opt, 5, "zone", "host")

	rc := checker.NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.AddLeaderRegion(1, 1, 2, 4)
	region := tc.GetRegion(1)

	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)
	peer6, _ := tc.AllocPeer(6)
	region = region.Clone(core.WithAddPeer(peer6))

	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 5)
	peer5, _ := tc.AllocPeer(5)
	region = region.Clone(core.WithAddPeer(peer5))

	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestStorageThreshold(c *C) {
	opt := mockoption.NewScheduleOptions()
	opt.LocationLabels = []string{"zone"}
	tc := mockcluster.NewCluster(opt)
	rc := checker.NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(1, 0.5, 0.5)
	tc.UpdateStoreRegionSize(1, 500*1024*1024)
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(2, 0.1, 0.9)
	tc.UpdateStoreRegionSize(2, 100*1024*1024)
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Move peer to better location.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 4)
	// If store4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.9, 0.1)
	c.Assert(rc.Check(region), IsNil)

	tc.AddLeaderRegion(2, 1, 3)
	region = tc.GetRegion(2)
	// Add peer on store4.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)
	// If store4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.8, 0)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
}

func (s *testReplicaCheckerSuite) TestOpts(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	rc := checker.NewReplicaChecker(tc)

	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)
	tc.AddRegionStore(4, 100)
	tc.AddLeaderRegion(1, 1, 2, 3)

	region := tc.GetRegion(1)
	// Test remove down replica and replace offline replica.
	tc.SetStoreDown(1)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        region.GetStorePeer(1),
			DownSeconds: 24 * 60 * 60,
		},
	}))
	tc.SetStoreOffline(2)
	// RemoveDownReplica has higher priority than replaceOfflineReplica.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 4)
	opt.EnableRemoveDownReplica = false
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 4)
	opt.EnableReplaceOfflineReplica = false
	c.Assert(rc.Check(region), IsNil)
}
