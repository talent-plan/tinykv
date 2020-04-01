// Copyright 2016 PingCAP, Inc.
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

package operator

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct {
	cluster *mockcluster.Cluster
}

func (s *testOperatorSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	s.cluster = mockcluster.NewCluster(cfg)
	stores := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	for _, storeID := range stores {
		s.cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: storeID}))
	}
}

func (s *testOperatorSuite) newTestRegion(regionID uint64, leaderPeer uint64, peers ...[2]uint64) *core.RegionInfo {
	var (
		region metapb.Region
		leader *metapb.Peer
	)
	region.Id = regionID
	for i := range peers {
		peer := &metapb.Peer{
			Id:      peers[i][1],
			StoreId: peers[i][0],
		}
		region.Peers = append(region.Peers, peer)
		if peer.GetId() == leaderPeer {
			leader = peer
		}
	}
	regionInfo := core.NewRegionInfo(&region, leader, core.SetApproximateSize(50))
	return regionInfo
}

func genAddPeers(store uint64, groups [][]uint64) [][]OpStep {
	ret := make([][]OpStep, len(groups))
	for i, grp := range groups {
		steps := make([]OpStep, len(grp))
		for j, id := range grp {
			steps[j] = AddPeer{ToStore: store, PeerID: id}
		}
		ret[i] = steps
	}
	return ret
}

func (s *testOperatorSuite) TestInterleaveStepGroups(c *C) {
	a := genAddPeers(1, [][]uint64{{1, 2}, {3}, {4, 5, 6}})
	b := genAddPeers(1, [][]uint64{{11}, {12}, {13, 14}, {15, 16}})
	ans := genAddPeers(1, [][]uint64{{1, 2, 11, 3, 12, 4, 5, 6, 13, 14, 15, 16}})
	res := interleaveStepGroups(a, b, 12)
	c.Assert(res, DeepEquals, ans[0])
}

func (s *testOperatorSuite) TestFindAvailableStore(c *C) {
	stores := []uint64{8, 7, 3, 4, 7, 3, 1, 5, 6}
	i, id := findAvailableStore(s.cluster, stores)
	c.Assert(i, Equals, 0)
	c.Assert(id, Equals, uint64(8))
	i, id = findAvailableStore(s.cluster, stores[2:])
	c.Assert(i, Equals, 0)
	c.Assert(id, Equals, uint64(3))
}

func (s *testOperatorSuite) TestOperatorStep(c *C) {
	region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	c.Assert(TransferLeader{FromStore: 1, ToStore: 2}.IsFinish(region), IsFalse)
	c.Assert(TransferLeader{FromStore: 2, ToStore: 1}.IsFinish(region), IsTrue)
	c.Assert(AddPeer{ToStore: 3, PeerID: 3}.IsFinish(region), IsFalse)
	c.Assert(AddPeer{ToStore: 1, PeerID: 1}.IsFinish(region), IsTrue)
	c.Assert(RemovePeer{FromStore: 1}.IsFinish(region), IsFalse)
	c.Assert(RemovePeer{FromStore: 3}.IsFinish(region), IsTrue)
}

func (s *testOperatorSuite) newTestOperator(regionID uint64, kind OpKind, steps ...OpStep) *Operator {
	return NewOperator("test", "test", regionID, &metapb.RegionEpoch{}, OpAdmin|kind, steps...)
}

func (s *testOperatorSuite) checkSteps(c *C, op *Operator, steps []OpStep) {
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		c.Assert(op.Step(i), Equals, steps[i])
	}
}

func (s *testOperatorSuite) TestOperator(c *C) {
	region := s.newTestRegion(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	// addPeer1, transferLeader1, removePeer3
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := s.newTestOperator(1, OpLeader|OpRegion, steps...)
	c.Assert(op.GetPriorityLevel(), Equals, core.HighPriority)
	s.checkSteps(c, op, steps)
	c.Assert(op.Check(region), IsNil)
	c.Assert(op.IsFinish(), IsTrue)
	op.startTime = time.Now()
	op.startTime = op.startTime.Add(-RegionOperatorWaitTime - time.Second)
	c.Assert(op.IsTimeout(), IsFalse)

	// addPeer1, transferLeader1, removePeer2
	steps = []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op = s.newTestOperator(1, OpLeader|OpRegion, steps...)
	s.checkSteps(c, op, steps)
	c.Assert(op.Check(region), Equals, RemovePeer{FromStore: 2})
	c.Assert(atomic.LoadInt32(&op.currentStep), Equals, int32(2))
	op.startTime = time.Now()
	c.Assert(op.IsTimeout(), IsFalse)
	op.startTime = op.startTime.Add(-LeaderOperatorWaitTime - time.Second)
	c.Assert(op.IsTimeout(), IsFalse)
	op.startTime = op.startTime.Add(-RegionOperatorWaitTime - time.Second)
	c.Assert(op.IsTimeout(), IsTrue)
	res, err := json.Marshal(op)
	c.Assert(err, IsNil)
	c.Assert(len(res), Equals, len(op.String())+2)

	// check short timeout for transfer leader only operators.
	steps = []OpStep{TransferLeader{FromStore: 2, ToStore: 1}}
	op = s.newTestOperator(1, OpLeader, steps...)
	op.startTime = time.Now()
	c.Assert(op.IsTimeout(), IsFalse)
	op.startTime = op.startTime.Add(-LeaderOperatorWaitTime - time.Second)
	c.Assert(op.IsTimeout(), IsTrue)
}

func (s *testOperatorSuite) TestOperatorKind(c *C) {
	c.Assert((OpLeader | OpReplica).String(), Equals, "leader,replica")
	c.Assert(OpKind(0).String(), Equals, "unknown")
	k, err := ParseOperatorKind("balance,region,leader")
	c.Assert(err, IsNil)
	c.Assert(k, Equals, OpBalance|OpRegion|OpLeader)
	_, err = ParseOperatorKind("leader,region")
	c.Assert(err, IsNil)
	_, err = ParseOperatorKind("foobar")
	c.Assert(err, NotNil)
}
