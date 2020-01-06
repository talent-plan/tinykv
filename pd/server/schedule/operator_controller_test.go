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

package schedule

import (
	"container/heap"
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockhbstream"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/checker"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOperatorControllerSuite{})

type testOperatorControllerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (t *testOperatorControllerSuite) SetUpSuite(c *C) {
	t.ctx, t.cancel = context.WithCancel(context.Background())
}

func (t *testOperatorControllerSuite) TearDownSuite(c *C) {
	t.cancel()
}

// issue #1338
func (t *testOperatorControllerSuite) TestGetOpInfluence(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(t.ctx, tc, nil)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
	}
	op1 := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op2 := operator.NewOperator("test", "test", 2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	oc.SetOperator(op1)
	oc.SetOperator(op2)
	go func(ctx context.Context) {
		c.Assert(oc.RemoveOperator(op1), IsTrue)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				c.Assert(oc.RemoveOperator(op1), IsFalse)
			}
		}
	}(t.ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				oc.GetOpInfluence(tc)
			}
		}
	}(t.ctx)
	time.Sleep(1 * time.Second)
	c.Assert(oc.GetOperator(2), NotNil)
}

func (t *testOperatorControllerSuite) TestOperatorStatus(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(t.ctx, tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op2 := operator.NewOperator("test", "test", 2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	op1.SetStartTime(time.Now())
	oc.SetOperator(op1)
	op2.SetStartTime(time.Now())
	oc.SetOperator(op2)
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_RUNNING)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	op1.SetStartTime(time.Now().Add(-10 * time.Minute))
	region2 = ApplyOperatorStep(region2, op2)
	tc.PutRegion(region2)
	oc.Dispatch(region1, "test")
	oc.Dispatch(region2, "test")
	c.Assert(oc.GetOperatorStatus(1).Status, Equals, pdpb.OperatorStatus_TIMEOUT)
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_RUNNING)
	ApplyOperator(tc, op2)
	oc.Dispatch(region2, "test")
	c.Assert(oc.GetOperatorStatus(2).Status, Equals, pdpb.OperatorStatus_SUCCESS)
}

// issue #1716
func (t *testOperatorControllerSuite) TestConcurrentRemoveOperator(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(t.ctx, tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 1},
		operator.AddPeer{ToStore: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	// unfinished op with high priority
	op2 := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion|operator.OpAdmin, steps...)

	oc.SetOperator(op1)

	c.Assert(failpoint.Enable("github.com/pingcap-incubator/tinykv/pd/server/schedule/concurrentRemoveOperator", "return(true)"), IsNil)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		oc.Dispatch(region1, "test")
		wg.Done()
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		success := oc.AddOperator(op2)
		// If the assert failed before wg.Done, the test will be blocked.
		defer c.Assert(success, IsTrue)
		wg.Done()
	}()
	wg.Wait()

	c.Assert(oc.GetOperator(1), Equals, op2)
}

func (t *testOperatorControllerSuite) TestPollDispatchRegion(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(t.ctx, tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	tc.AddLeaderRegion(4, 2, 1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	op2 := operator.NewOperator("test", "test", 2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op3 := operator.NewOperator("test", "test", 3, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op4 := operator.NewOperator("test", "test", 4, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	region4 := tc.GetRegion(4)
	// Adds operator and pushes to the notifier queue.
	{
		oc.SetOperator(op1)
		oc.SetOperator(op3)
		oc.SetOperator(op4)
		oc.SetOperator(op2)
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op1, time: time.Now().Add(100 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op3, time: time.Now().Add(300 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op4, time: time.Now().Add(499 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op2, time: time.Now().Add(500 * time.Millisecond)})
	}
	// fisrt poll got nil
	r, next := oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsFalse)

	// after wait 100 millisecond, the region1 need to dispatch, but not region2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region1.GetID())

	// find op3 with nil region, remove it
	c.Assert(oc.GetOperator(3), NotNil)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsTrue)
	c.Assert(oc.GetOperator(3), IsNil)

	// find op4 finished
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region4.GetID())

	// after waiting 500 millseconds, the region2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, NotNil)
	c.Assert(next, IsTrue)
	c.Assert(r.GetID(), Equals, region2.GetID())
	r, next = oc.pollNeedDispatchRegion()
	c.Assert(r, IsNil)
	c.Assert(next, IsFalse)
}

func (t *testOperatorControllerSuite) TestStoreLimit(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(t.ctx, tc, mockhbstream.NewHeartbeatStream())
	tc.AddLeaderStore(1, 0)
	tc.UpdateLeaderCount(1, 1000)
	tc.AddLeaderStore(2, 0)
	for i := uint64(1); i <= 1000; i++ {
		tc.AddLeaderRegion(i, i)
	}
	oc.SetStoreLimit(2, 1)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op := operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 1})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)

	oc.SetStoreLimit(2, 2)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewOperator("test", "test", i, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	oc.SetAllStoresLimit(1)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewOperator("test", "test", i, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op = operator.NewOperator("test", "test", 1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 1})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)
}

// #1652
func (t *testOperatorControllerSuite) TestDispatchOutdatedRegion(c *C) {
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	stream := mockhbstream.NewHeartbeatStreams(cluster.ID)
	controller := NewOperatorController(t.ctx, cluster, stream)

	cluster.AddLeaderStore(1, 2)
	cluster.AddLeaderStore(2, 0)
	cluster.AddLeaderRegion(1, 1, 2)
	steps := []operator.OpStep{
		operator.TransferLeader{FromStore: 1, ToStore: 2},
		operator.RemovePeer{FromStore: 1},
	}

	op := operator.NewOperator("test", "test", 1,
		&metapb.RegionEpoch{ConfVer: 0, Version: 0},
		operator.OpRegion, steps...)
	c.Assert(controller.AddOperator(op), Equals, true)
	c.Assert(len(stream.MsgCh()), Equals, 1)

	// report the result of transferring leader
	region := cluster.MockRegionInfo(1, 2, []uint64{1, 2},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region), Equals, 0)
	c.Assert(len(stream.MsgCh()), Equals, 2)

	// report the result of removing peer
	region = cluster.MockRegionInfo(1, 2, []uint64{2},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region), Equals, 1)
	c.Assert(len(stream.MsgCh()), Equals, 2)

	// add and disaptch op again, the op should be stale
	op = operator.NewOperator("test", "test", 1,
		&metapb.RegionEpoch{ConfVer: 0, Version: 0},
		operator.OpRegion, steps...)
	c.Assert(controller.AddOperator(op), Equals, true)
	c.Assert(op.ConfVerChanged(region), Equals, 0)
	c.Assert(len(stream.MsgCh()), Equals, 3)

	// report region with an abnormal confver
	region = cluster.MockRegionInfo(1, 1, []uint64{1, 2},
		&metapb.RegionEpoch{ConfVer: 1, Version: 0})
	controller.Dispatch(region, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region), Equals, 0)
	// no new step
	c.Assert(len(stream.MsgCh()), Equals, 3)
}

func (t *testOperatorControllerSuite) TestDispatchUnfinishedStep(c *C) {
	cluster := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	stream := mockhbstream.NewHeartbeatStreams(cluster.ID)
	controller := NewOperatorController(t.ctx, cluster, stream)

	// Create a new region with epoch(0, 0)
	// the region has two peers with its peer id allocated incrementally.
	// so the two peers are {peerid: 1, storeid: 1}, {peerid: 2, storeid: 2}
	// The peer on store 1 is the leader
	epoch := &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	region := cluster.MockRegionInfo(1, 1, []uint64{2}, epoch)
	// Put region into cluster, otherwise, AddOperator will fail because of
	// missing region
	cluster.PutRegion(region)

	// The next allocated peer should have peerid 3, so we add this peer
	// to store 3
	steps := []operator.OpStep{
		operator.AddLearner{ToStore: 3, PeerID: 3},
		operator.PromoteLearner{ToStore: 3, PeerID: 3},
		operator.TransferLeader{ToStore: 3},
		operator.RemovePeer{FromStore: 1},
	}

	// Create an operator
	op := operator.NewOperator("test", "test", 1, epoch,
		operator.OpRegion, steps...)
	c.Assert(controller.AddOperator(op), Equals, true)
	c.Assert(len(stream.MsgCh()), Equals, 1)

	// Create region2 witch is cloned from the original region.
	// region2 has peer 3 in pending state, so the AddPeer step
	// is left unfinished
	region2 := region.Clone(
		core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, IsLearner: true}),
		core.WithPendingPeers([]*metapb.Peer{
			{Id: 3, StoreId: 3, IsLearner: true},
		}),
		core.WithIncConfVer(),
	)
	c.Assert(region2.GetPendingPeers(), NotNil)
	c.Assert(steps[0].IsFinish(region2), Equals, false)
	controller.Dispatch(region2, DispatchFromHeartBeat)

	// In this case, the conf version has been changed, but the
	// peer added is in peeding state, the operator should not be
	// removed by the stale checker
	c.Assert(op.ConfVerChanged(region2), Equals, 1)
	c.Assert(controller.GetOperator(1), NotNil)
	// The operator is valid yet, but the step should not be sent
	// again, because it is in pending state, so the message channel
	// should not be increased
	c.Assert(len(stream.MsgCh()), Equals, 1)

	// Finish the step by clearing the pending state
	region3 := region.Clone(
		core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, IsLearner: true}),
		core.WithIncConfVer(),
	)
	c.Assert(steps[0].IsFinish(region3), Equals, true)
	controller.Dispatch(region3, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region3), Equals, 1)
	c.Assert(len(stream.MsgCh()), Equals, 2)

	region4 := region3.Clone(
		core.WithPromoteLearner(3),
		core.WithIncConfVer(),
	)
	c.Assert(steps[1].IsFinish(region4), Equals, true)
	controller.Dispatch(region4, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region4), Equals, 2)
	c.Assert(len(stream.MsgCh()), Equals, 3)

	// Transfer leader
	region5 := region4.Clone(
		core.WithLeader(region4.GetStorePeer(3)),
	)
	c.Assert(steps[2].IsFinish(region5), Equals, true)
	controller.Dispatch(region5, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region5), Equals, 2)
	c.Assert(len(stream.MsgCh()), Equals, 4)

	// Remove peer
	region6 := region5.Clone(
		core.WithRemoveStorePeer(1),
		core.WithIncConfVer(),
	)
	c.Assert(steps[3].IsFinish(region6), Equals, true)
	controller.Dispatch(region6, DispatchFromHeartBeat)
	c.Assert(op.ConfVerChanged(region6), Equals, 3)

	// The Operator has finished, so no message should be sent
	c.Assert(len(stream.MsgCh()), Equals, 4)
	c.Assert(controller.GetOperator(1), IsNil)
}

func (t *testOperatorControllerSuite) TestStoreLimitWithMerge(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	tc := mockcluster.NewCluster(cfg)
	regions := []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	tc.AddLeaderStore(1, 10)
	tc.AddLeaderStore(4, 10)
	tc.AddLeaderStore(5, 10)
	for _, region := range regions {
		tc.PutRegion(region)
	}

	mc := checker.NewMergeChecker(t.ctx, tc)
	oc := NewOperatorController(t.ctx, tc, mockhbstream.NewHeartbeatStream())

	cfg.StoreBalanceRate = 60

	regions[2] = regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 109, StoreId: 2},
			{Id: 110, StoreId: 3},
			{Id: 111, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}),
	)
	tc.PutRegion(regions[2])
	ops := mc.Check(regions[2])
	c.Assert(ops, NotNil)
	// The size of Region is less or equal than 1MB.
	for i := 0; i < 50; i++ {
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	regions[2] = regions[2].Clone(
		core.SetApproximateSize(2),
		core.SetApproximateKeys(2),
	)
	tc.PutRegion(regions[2])
	ops = mc.Check(regions[2])
	c.Assert(ops, NotNil)
	// The size of Region is more than 1MB but no more than 20MB.
	for i := 0; i < 5; i++ {
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	c.Assert(oc.AddOperator(ops...), IsFalse)
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	var prs []*metapb.Peer
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}
