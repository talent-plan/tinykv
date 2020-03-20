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
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
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

func newTestReplication(mso *mockoption.ScheduleOptions, maxReplicas int) {
	mso.MaxReplicas = maxReplicas
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

func (s *testBalanceRegionSchedulerSuite) TestReplicas13C(c *C) {
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
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc), operator.OpBalance, 4, 1)

	// Test stateFilter.
	tc.SetStoreOffline(1)
	tc.UpdateRegionCount(2, 6)

	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	testutil.CheckTransferPeerWithLeaderTransfer(c, sb.Schedule(tc), operator.OpBalance, 4, 2)
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(tc), IsNil)

	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(tc), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas33C(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 3)

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.AddRegionStore(1, 16)
	tc.AddRegionStore(2, 15)
	tc.AddRegionStore(3, 14)

	tc.AddLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores.
	c.Assert(sb.Schedule(tc), IsNil)

	// Store 4 has smaller region score than store 1.
	tc.AddRegionStore(4, 2)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 1, 4)

	// Store 5 has smaller region score than store 4.
	tc.AddRegionStore(5, 1)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 1, 5)

	// Store 6 has smaller region score with store 6.
	tc.AddRegionStore(6, 0)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 1, 6)

	// If store 6 is not available, will choose store 5.
	tc.SetStoreDown(6)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 1, 5)

	// Take down 4,5,6
	tc.SetStoreDown(4)
	tc.SetStoreDown(5)
	tc.SetStoreDown(6)

	// Store 7 has different zone with other stores but larger region score than store 1.
	tc.AddRegionStore(7, 20)
	c.Assert(sb.Schedule(tc), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas53C(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 5)

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	tc.AddRegionStore(1, 4)
	tc.AddRegionStore(2, 5)
	tc.AddRegionStore(3, 6)
	tc.AddRegionStore(4, 7)
	tc.AddRegionStore(5, 28)

	tc.AddLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.AddRegionStore(6, 1)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 5, 6)

	// Store 7 has larger region score and same distinct score with store 6.
	tc.AddRegionStore(7, 5)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 5, 6)

	// Store 1 has smaller region score and higher distinct score.
	tc.AddLeaderRegion(1, 2, 3, 4, 5, 6)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.AddRegionStore(11, 29)
	tc.AddRegionStore(12, 8)
	tc.AddRegionStore(13, 7)
	tc.AddLeaderRegion(1, 2, 3, 11, 12, 13)
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 11, 6)
}

func (s *testBalanceRegionSchedulerSuite) TestReplacePendingRegion3C(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, nil, nil)

	newTestReplication(opt, 3)

	sb, err := schedule.CreateScheduler("balance-region", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.AddRegionStore(1, 16)
	tc.AddRegionStore(2, 7)
	tc.AddRegionStore(3, 15)
	// Store 4 has smaller region score than store 1 and more better place than store 2.
	tc.AddRegionStore(4, 10)

	// set pending peer
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 2, 1, 3)
	region := tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(1)}))
	tc.PutRegion(region)

	c.Assert(sb.Schedule(tc).RegionID(), Equals, uint64(3))
	testutil.CheckTransferPeer(c, sb.Schedule(tc), operator.OpBalance, 1, 4)
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

func (s *testBalanceLeaderSchedulerSuite) schedule() *operator.Operator {
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
	// Leaders:    7    8    9   18
	// Region1:    F    F    F    L
	s.tc.UpdateLeaderCount(4, 18)
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
	c.Check(s.schedule(), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLeaderTolerantRatio(c *C) {
	// default leader tolerant ratio is 5, when schedule by count
	// Stores:			1		2    	3    	4
	// Leader Count:		14->21		10    	10    	10
	// Leader Size :		100		100    	100    	100
	// Region1:			L		F   	F    	F
	s.tc.AddLeaderStore(1, 14, 100)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderRegion(1, 1, 2, 3, 4)
	c.Check(s.schedule(), IsNil)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 14)
	s.tc.AddLeaderStore(1, 21, 100)
	c.Assert(s.tc.GetStore(1).GetLeaderCount(), Equals, 21)

	c.Check(s.schedule(), NotNil)
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

	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 1)
	// Test stateFilter.
	// if store 4 is offline, we should consider it
	// because it still provides services
	s.tc.SetStoreOffline(4)
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 1)
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.SetStoreDown(1)
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.SetStoreBusy(2, true)
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 3)

	// Test disconnectFilter.
	// If store 3 is disconnected, no operator can be created.
	s.tc.SetStoreDisconnect(3)
	c.Assert(s.schedule(), IsNil)
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
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.UpdateLeaderCount(2, 14)
	s.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of store4, move a leader into store1.
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    L    F
	// Region2:    L    F    F    -
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderRegion(1, 3, 2, 4)
	s.tc.AddLeaderRegion(2, 1, 2, 3)
	// No leader in store16, no follower in store1. Now source and target are store3 and store2.
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 3, 2)

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
	testutil.CheckTransferLeader(c, s.schedule(), operator.OpBalance, 4, 3)
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

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.SetStoreDown(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	tc.SetStoreUp(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	region = region.Clone(core.WithAddPeer(peer4))
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	region = region.Clone(core.WithAddPeer(peer3))
	testutil.CheckRemovePeer(c, rc.Check(region), 1)

	region = region.Clone(core.WithRemoveStorePeer(1))

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

	newTestReplication(opt, 3)

	rc := checker.NewReplicaChecker(tc)

	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 3)
	tc.AddRegionStore(4, 4)

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

	// Store 5 has smaller region score than store 4, we will choose store 5.
	tc.AddRegionStore(5, 3)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 5)
}
