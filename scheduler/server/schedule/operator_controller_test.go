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
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockhbstream"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	. "github.com/pingcap/check"
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
