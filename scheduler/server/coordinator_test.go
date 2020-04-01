// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockhbstream"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockid"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedulers"
	. "github.com/pingcap/check"
)

func newTestScheduleConfig() (*config.ScheduleConfig, *config.ScheduleOption, error) {
	cfg := config.NewConfig()
	if err := cfg.Adjust(nil); err != nil {
		return nil, nil, err
	}
	opt := config.NewScheduleOption(cfg)
	return &cfg.Schedule, opt, nil
}

func newTestOperator(regionID uint64, regionEpoch *metapb.RegionEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewOperator("test", "test", regionID, regionEpoch, kind, steps...)
}

type testCluster struct {
	*RaftCluster
}

func newTestCluster(opt *config.ScheduleOption) *testCluster {
	cluster := createTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()))
	return &testCluster{RaftCluster: cluster}
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
	}
}

func (c *testCluster) addRegionStore(storeID uint64, regionCount int, regionSizes ...uint64) error {
	var regionSize uint64
	if len(regionSizes) == 0 {
		regionSize = uint64(regionCount) * 10
	} else {
		regionSize = regionSizes[0]
	}

	stats := &schedulerpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - regionSize
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionSize)),
		core.SetLastHeartbeatTS(time.Now()),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderRegion(regionID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) error {
	region := newTestRegionMeta(regionID)
	leader, _ := c.AllocPeer(leaderStoreID)
	region.Peers = []*metapb.Peer{leader}
	for _, followerStoreID := range followerStoreIDs {
		peer, _ := c.AllocPeer(followerStoreID)
		region.Peers = append(region.Peers, peer)
	}
	regionInfo := core.NewRegionInfo(region, leader, core.SetApproximateSize(10))
	return c.putRegion(regionInfo)
}

func (c *testCluster) updateLeaderCount(storeID uint64, leaderCount int) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderStore(storeID uint64, leaderCount int) error {
	stats := &schedulerpb.StoreStats{}
	newStore := core.NewStoreInfo(&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) LoadRegion(regionID uint64, followerStoreIDs ...uint64) error {
	//  regions load from etcd will have no leader
	region := newTestRegionMeta(regionID)
	region.Peers = []*metapb.Peer{}
	for _, id := range followerStoreIDs {
		peer, _ := c.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	return c.putRegion(core.NewRegionInfo(region, nil))
}

func waitOperator(c *C, co *coordinator, regionID uint64) {
	testutil.WaitUntil(c, func(c *C) bool {
		return co.opController.GetOperator(regionID) != nil
	})
}

var _ = Suite(&testCoordinatorSuite{})

type testCoordinatorSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testCoordinatorSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testCoordinatorSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testCoordinatorSuite) TestBasic(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	oc := co.opController

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)

	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddOperator(op1)
	c.Assert(oc.OperatorCount(op1.Kind()), Equals, uint64(1))
	c.Assert(oc.GetOperator(1).RegionID(), Equals, op1.RegionID())

	// Region 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddOperator(op2)
	c.Assert(oc.OperatorCount(op2.Kind()), Equals, uint64(0))

	// Remove the operator manually, then we can add a new operator.
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	oc.AddOperator(op2)
	c.Assert(oc.OperatorCount(op2.Kind()), Equals, uint64(1))
	c.Assert(oc.GetOperator(1).RegionID(), Equals, op2.RegionID())
}

func (s *testCoordinatorSuite) TestDispatch(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addRegionStore(3, 30), IsNil)
	c.Assert(tc.addRegionStore(2, 20), IsNil)
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)

	// Transfer leader from store 4 to store 2.
	c.Assert(tc.updateLeaderCount(4, 50), IsNil)
	c.Assert(tc.updateLeaderCount(3, 30), IsNil)
	c.Assert(tc.updateLeaderCount(2, 20), IsNil)
	c.Assert(tc.updateLeaderCount(1, 10), IsNil)
	c.Assert(tc.addLeaderRegion(2, 4, 3, 2), IsNil)

	// Wait for schedule and turn off balance.
	waitOperator(c, co, 1)
	testutil.CheckTransferPeer(c, co.opController.GetOperator(1), operator.OpBalance, 4, 1)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	waitOperator(c, co, 2)
	testutil.CheckTransferLeader(c, co.opController.GetOperator(2), operator.OpBalance, 4, 2)
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	region := tc.GetRegion(1).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddPeer(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitRemovePeer(c, stream, region, 4)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Transfer leader.
	region = tc.GetRegion(2).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitTransferLeader(c, stream, region, 2)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func dispatchHeartbeat(c *C, co *coordinator, region *core.RegionInfo, stream mockhbstream.HeartbeatStream) error {
	co.hbStreams.bindStream(region.GetLeader().GetStoreId(), stream)
	if err := co.cluster.putRegion(region.Clone()); err != nil {
		return err
	}
	co.opController.Dispatch(region, schedule.DispatchFromHeartBeat)
	return nil
}

func (s *testCoordinatorSuite) TestReplica(c *C) {
	// Turn off balance.
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addRegionStore(4, 4), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to store 1.
	c.Assert(tc.addLeaderRegion(1, 2, 3), IsNil)
	region := tc.GetRegion(1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddPeer(c, stream, region, 1)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Remove peer from store 4.
	c.Assert(tc.addLeaderRegion(2, 1, 2, 3, 4), IsNil)
	region = tc.GetRegion(2)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitRemovePeer(c, stream, region, 4)
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)

	// Remove offline peer directly when it's pending.
	c.Assert(tc.addLeaderRegion(3, 1, 2, 3), IsNil)
	c.Assert(tc.setStoreOffline(3), IsNil)
	region = tc.GetRegion(3)
	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestPeerState(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	defer co.wg.Wait()
	defer co.stop()

	// Transfer peer from store 4 to store 1.
	c.Assert(tc.addRegionStore(1, 10), IsNil)
	c.Assert(tc.addRegionStore(2, 20), IsNil)
	c.Assert(tc.addRegionStore(3, 30), IsNil)
	c.Assert(tc.addRegionStore(4, 40), IsNil)
	c.Assert(tc.addLeaderRegion(1, 2, 3, 4), IsNil)

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(c, co, 1)
	testutil.CheckTransferPeer(c, co.opController.GetOperator(1), operator.OpBalance, 4, 1)

	region := tc.GetRegion(1).Clone()

	// Add new peer.
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddPeer(c, stream, region, 1)

	// If the new peer is pending, the operator will not finish.
	region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), region.GetStorePeer(1))))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
	c.Assert(co.opController.GetOperator(region.GetID()), NotNil)

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	region = region.Clone(core.WithPendingPeers(nil))
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitRemovePeer(c, stream, region, 4)
	c.Assert(tc.addLeaderRegion(1, 1, 2, 3), IsNil)
	region = tc.GetRegion(1).Clone()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	waitNoResponse(c, stream)
}

func (s *testCoordinatorSuite) TestRemoveScheduler(c *C) {
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.ReplicaScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()

	// Add stores 1,2
	c.Assert(tc.addLeaderStore(1, 1), IsNil)
	c.Assert(tc.addLeaderStore(2, 1), IsNil)

	c.Assert(co.schedulers, HasLen, 2)
	storage := tc.RaftCluster.storage

	sches, _, err := storage.LoadAllScheduleConfig()
	c.Assert(err, IsNil)
	c.Assert(sches, HasLen, 2)

	// remove all schedulers
	c.Assert(co.removeScheduler("balance-leader-scheduler"), IsNil)
	c.Assert(co.removeScheduler("balance-region-scheduler"), IsNil)
	// all removed
	sches, _, err = storage.LoadAllScheduleConfig()
	c.Assert(err, IsNil)
	c.Assert(sches, HasLen, 0)
	c.Assert(co.schedulers, HasLen, 0)
	newOpt := co.cluster.opt
	co.stop()
	co.wg.Wait()

	// suppose restart PD again
	c.Assert(err, IsNil)
	tc.RaftCluster.opt = newOpt
	co = newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	c.Assert(co.schedulers, HasLen, 0)
	// the option remains default scheduler
	c.Assert(co.cluster.opt.GetSchedulers(), HasLen, 2)
	co.stop()
	co.wg.Wait()
}

func (s *testCoordinatorSuite) TestRestart(c *C) {
	// Turn off balance, we test add replica only.
	cfg, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cfg.LeaderScheduleLimit = 0
	cfg.RegionScheduleLimit = 0

	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	// Add 3 stores (1, 2, 3) and a region with 1 replica on store 1.
	c.Assert(tc.addRegionStore(1, 1), IsNil)
	c.Assert(tc.addRegionStore(2, 2), IsNil)
	c.Assert(tc.addRegionStore(3, 3), IsNil)
	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	region := tc.GetRegion(1)

	// Add 1 replica on store 2.
	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	stream := mockhbstream.NewHeartbeatStream()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddPeer(c, stream, region, 2)
	co.stop()
	co.wg.Wait()

	// Recreate coodinator then add another replica on store 3.
	co = newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	co.run()
	c.Assert(dispatchHeartbeat(c, co, region, stream), IsNil)
	region = waitAddPeer(c, stream, region, 3)
	co.stop()
	co.wg.Wait()
}

var _ = Suite(&testOperatorControllerSuite{})

type testOperatorControllerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testOperatorControllerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testOperatorControllerSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testOperatorControllerSuite) TestOperatorCount(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams := mockhbstream.NewHeartbeatStreams(tc.RaftCluster.getClusterID())
	oc := schedule.NewOperatorController(s.ctx, tc.RaftCluster, hbStreams)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(0))
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(0))

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	c.Assert(tc.addLeaderRegion(2, 2), IsNil)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	oc.AddOperator(op1)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1)) // 1:leader
	op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
	oc.AddOperator(op2)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(2)) // 1:leader, 2:leader
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1)) // 2:leader

	op1 = newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpRegion)
	oc.AddOperator(op1)
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(1)) // 1:region 2:leader
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(1))
	op2 = newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpRegion)
	op2.SetPriorityLevel(core.HighPriority)
	oc.AddOperator(op2)
	c.Assert(oc.OperatorCount(operator.OpRegion), Equals, uint64(2)) // 1:region 2:region
	c.Assert(oc.OperatorCount(operator.OpLeader), Equals, uint64(0))
}

var _ = Suite(&testScheduleControllerSuite{})

type testScheduleControllerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testScheduleControllerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testScheduleControllerSuite) TearDownSuite(c *C) {
	s.cancel()
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.OperatorController
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func (s *testScheduleControllerSuite) TestController(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	c.Assert(tc.addLeaderRegion(1, 1), IsNil)
	c.Assert(tc.addLeaderRegion(2, 2), IsNil)

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	oc := co.opController
	scheduler, err := schedule.CreateScheduler("balance-leader", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		c.Assert(sc.GetInterval(), Equals, i)
		c.Assert(sc.Schedule(), IsNil)
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	c.Assert(sc.AllowSchedule(), IsTrue)
	op1 := newTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
	op2 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddOperator(op2), IsTrue)
	// count = 2
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)

	// add a PriorityKind operator will remove old operator
	op3 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpBalance)
	op3.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddOperator(op1), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.AddOperator(op3), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op3), IsTrue)

	// add a admin operator will remove old operator
	c.Assert(oc.AddOperator(op2), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	op4 := newTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
	op4.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddOperator(op4), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op4), IsTrue)

	// test wrong region id.
	op5 := newTestOperator(3, &metapb.RegionEpoch{}, operator.OpBalance)
	c.Assert(oc.AddOperator(op5), IsFalse)

	// test wrong region epoch.
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	op6 := newTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddOperator(op6), IsFalse)
	epoch.Version--
	op6 = newTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddOperator(op6), IsTrue)
	c.Assert(oc.RemoveOperator(op6), IsTrue)
}

func (s *testScheduleControllerSuite) TestInterval(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	tc := newTestCluster(opt)
	hbStreams, cleanup := getHeartBeatStreams(s.ctx, c, tc)
	defer cleanup()
	defer hbStreams.Close()

	co := newCoordinator(s.ctx, tc.RaftCluster, hbStreams)
	lb, err := schedule.CreateScheduler("balance-leader", co.opController, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			c.Assert(sc.Schedule(), IsNil)
		}
		c.Assert(sc.GetInterval(), Less, time.Second*time.Duration(n/2))
	}
}

func waitAddPeer(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *schedulerpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_AddNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithAddPeer(res.GetChangePeer().GetPeer()),
		core.WithIncConfVer(),
	)
}

func waitRemovePeer(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *schedulerpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() &&
				res.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_RemoveNode &&
				res.GetChangePeer().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithIncConfVer(),
	)
}

func waitTransferLeader(c *C, stream mockhbstream.HeartbeatStream, region *core.RegionInfo, storeID uint64) *core.RegionInfo {
	var res *schedulerpb.RegionHeartbeatResponse
	testutil.WaitUntil(c, func(c *C) bool {
		if res = stream.Recv(); res != nil {
			return res.GetRegionId() == region.GetID() && res.GetTransferLeader().GetPeer().GetStoreId() == storeID
		}
		return false
	})
	return region.Clone(
		core.WithLeader(res.GetTransferLeader().GetPeer()),
	)
}

func waitNoResponse(c *C, stream mockhbstream.HeartbeatStream) {
	testutil.WaitUntil(c, func(c *C) bool {
		res := stream.Recv()
		return res == nil
	})
}

func getHeartBeatStreams(ctx context.Context, c *C, tc *testCluster) (*heartbeatStreams, func()) {
	config := NewTestSingleConfig(c)
	svr, err := CreateServer(config)
	c.Assert(err, IsNil)
	kvBase := kv.NewEtcdKVBase(svr.client, svr.rootPath)
	c.Assert(err, IsNil)
	svr.storage = core.NewStorage(kvBase)
	cluster := tc.RaftCluster
	cluster.s = svr
	cluster.running = false
	cluster.clusterID = tc.getClusterID()
	cluster.clusterRoot = svr.getClusterRootPath()
	hbStreams := newHeartbeatStreams(ctx, tc.getClusterID(), cluster)
	return hbStreams, func() { testutil.CleanServer(config) }
}

func createTestRaftCluster(id id.Allocator, opt *config.ScheduleOption, storage *core.Storage) *RaftCluster {
	cluster := &RaftCluster{}
	cluster.initCluster(id, opt, storage)
	return cluster
}
