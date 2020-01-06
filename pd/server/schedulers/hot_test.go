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

package schedulers

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockcluster"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/pd/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/server/kv"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule"
	"github.com/pingcap-incubator/tinykv/pd/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/pd/server/statistics"
)

var _ = Suite(&testHotWriteRegionSchedulerSuite{})

type testHotWriteRegionSchedulerSuite struct{}

func (s *testHotWriteRegionSchedulerSuite) TestSchedule(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := mockoption.NewScheduleOptions()
	newTestReplication(opt, 3, "zone", "host")
	tc := mockcluster.NewCluster(opt)
	hb, err := schedule.CreateScheduler("hot-write-region", schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5, 6  with region counts 3, 2, 2, 2, 0, 0.

	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.SetStoreDown(7)

	// Report store written bytes.
	tc.UpdateStorageWrittenBytes(1, 75*1024*1024)
	tc.UpdateStorageWrittenBytes(2, 45*1024*1024)
	tc.UpdateStorageWrittenBytes(3, 45*1024*1024)
	tc.UpdateStorageWrittenBytes(4, 60*1024*1024)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 3, 4)
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 4)
	opt.HotRegionCacheHitsThreshold = 0

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	op := hb.Schedule(tc)
	c.Assert(op, NotNil)
	switch op[0].Len() {
	case 1:
		// balance by leader selected
		testutil.CheckTransferLeaderFrom(c, op[0], operator.OpHotRegion, 1)
	case 4:
		// balance by peer selected
		if op[0].RegionID() == 2 {
			// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(c, op[0], operator.OpHotRegion, 1)
		} else {
			// peer in store 1 of the region 1,2 can only transfer to store 6
			testutil.CheckTransferPeerWithLeaderTransfer(c, op[0], operator.OpHotRegion, 1, 6)
		}
	}

	// hot region scheduler is restricted by `hot-region-schedule-limit`.
	opt.HotRegionScheduleLimit = 0
	c.Assert(hb.Schedule(tc), HasLen, 0)
	// hot region scheduler is not affect by `balance-region-schedule-limit`.
	opt.HotRegionScheduleLimit = mockoption.NewScheduleOptions().HotRegionScheduleLimit
	opt.RegionScheduleLimit = 0
	c.Assert(hb.Schedule(tc), HasLen, 1)
	// Always produce operator
	c.Assert(hb.Schedule(tc), HasLen, 1)
	c.Assert(hb.Schedule(tc), HasLen, 1)

	//| region_id | leader_store | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        2       |       3        |      512KB    |
	//|     3     |       6      |        1       |       4        |      512KB    |
	//|     4     |       5      |        6       |       4        |      512KB    |
	//|     5     |       3      |        4       |       5        |      512KB    |
	tc.UpdateStorageWrittenBytes(1, 60*1024*1024)
	tc.UpdateStorageWrittenBytes(2, 30*1024*1024)
	tc.UpdateStorageWrittenBytes(3, 60*1024*1024)
	tc.UpdateStorageWrittenBytes(4, 30*1024*1024)
	tc.UpdateStorageWrittenBytes(5, 0*1024*1024)
	tc.UpdateStorageWrittenBytes(6, 30*1024*1024)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(3, 6, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 4)
	tc.AddLeaderRegionWithWriteInfo(4, 5, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 6, 4)
	tc.AddLeaderRegionWithWriteInfo(5, 3, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 4, 5)
	// We can find that the leader of all hot regions are on store 1,
	// so one of the leader will transfer to another store.
	op = hb.Schedule(tc)
	if op != nil {
		testutil.CheckTransferLeaderFrom(c, op[0], operator.OpHotRegion, 1)
	}

	// hot region scheduler is restricted by schedule limit.
	opt.LeaderScheduleLimit = 0
	c.Assert(hb.Schedule(tc), HasLen, 0)
	opt.LeaderScheduleLimit = mockoption.NewScheduleOptions().LeaderScheduleLimit

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
}

var _ = Suite(&testHotReadRegionSchedulerSuite{})

type testHotReadRegionSchedulerSuite struct{}

func (s *testHotReadRegionSchedulerSuite) TestSchedule(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	hb, err := schedule.CreateScheduler("hot-read-region", schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	// Report store read bytes.
	tc.UpdateStorageReadBytes(1, 75*1024*1024)
	tc.UpdateStorageReadBytes(2, 45*1024*1024)
	tc.UpdateStorageReadBytes(3, 45*1024*1024)
	tc.UpdateStorageReadBytes(4, 60*1024*1024)
	tc.UpdateStorageReadBytes(5, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_store | follower_store | follower_store |   read_bytes  |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       2      |        1       |       3        |      512KB    |
	//|     3     |       1      |        2       |       3        |      512KB    |
	tc.AddLeaderRegionWithReadInfo(1, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(2, 2, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 3)
	tc.AddLeaderRegionWithReadInfo(3, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	// lower than hot read flow rate, but higher than write flow rate
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	opt.HotRegionCacheHitsThreshold = 0
	c.Assert(tc.IsRegionHot(tc.GetRegion(1)), IsTrue)
	c.Assert(tc.IsRegionHot(tc.GetRegion(11)), IsFalse)
	// check randomly pick hot region
	r := tc.RandHotRegionFromStore(2, statistics.ReadFlow)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(2))
	// check hot items
	stats := tc.HotCache.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats), Equals, 2)
	for _, ss := range stats {
		for _, s := range ss {
			c.Assert(s.BytesRate, Equals, 512.0*1024)
		}
	}
	// Will transfer a hot region leader from store 1 to store 3, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	testutil.CheckTransferLeader(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 3)
	// assume handle the operator
	tc.AddLeaderRegionWithReadInfo(3, 3, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 2)

	// After transfer a hot region leader from store 1 to store 3
	// the tree region leader will be evenly distributed in three stores
	tc.UpdateStorageReadBytes(1, 60*1024*1024)
	tc.UpdateStorageReadBytes(2, 30*1024*1024)
	tc.UpdateStorageReadBytes(3, 60*1024*1024)
	tc.UpdateStorageReadBytes(4, 30*1024*1024)
	tc.UpdateStorageReadBytes(5, 30*1024*1024)
	tc.AddLeaderRegionWithReadInfo(4, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(5, 4, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 5)

	// Now appear two read hot region in store 1 and 4
	// We will Transfer peer from 1 to 5
	testutil.CheckTransferPeerWithLeaderTransfer(c, hb.Schedule(tc)[0], operator.OpHotRegion, 1, 5)

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Regions.RemoveRegion(tc.GetRegion(i))
	}
	hb.Schedule(tc)
}

var _ = Suite(&testHotCacheSuite{})

type testHotCacheSuite struct{}

func (s *testHotCacheSuite) TestUpdateCache(c *C) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	// Report store read bytes.
	tc.UpdateStorageReadBytes(1, 75*1024*1024)
	tc.UpdateStorageReadBytes(2, 45*1024*1024)
	tc.UpdateStorageReadBytes(3, 45*1024*1024)
	tc.UpdateStorageReadBytes(4, 60*1024*1024)
	tc.UpdateStorageReadBytes(5, 0)

	/// For read flow
	tc.AddLeaderRegionWithReadInfo(1, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(2, 2, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 1, 3)
	tc.AddLeaderRegionWithReadInfo(3, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	// lower than hot read flow rate, but higher than write flow rate
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	opt.HotRegionCacheHitsThreshold = 0
	stats := tc.RegionStats(statistics.ReadFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 1)
	c.Assert(len(stats[3]), Equals, 0)

	tc.AddLeaderRegionWithReadInfo(3, 2, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithReadInfo(11, 1, 24*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	stats = tc.RegionStats(statistics.ReadFlow)

	c.Assert(len(stats[1]), Equals, 1)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 0)

	// For write flow
	tc.UpdateStorageWrittenBytes(1, 60*1024*1024)
	tc.UpdateStorageWrittenBytes(2, 30*1024*1024)
	tc.UpdateStorageWrittenBytes(3, 60*1024*1024)
	tc.UpdateStorageWrittenBytes(4, 30*1024*1024)
	tc.UpdateStorageWrittenBytes(5, 0*1024*1024)
	tc.AddLeaderRegionWithWriteInfo(4, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(5, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)
	tc.AddLeaderRegionWithWriteInfo(6, 1, 12*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 3)

	stats = tc.RegionStats(statistics.WriteFlow)
	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 2)

	tc.AddLeaderRegionWithWriteInfo(5, 1, 512*1024*statistics.RegionHeartBeatReportInterval, statistics.RegionHeartBeatReportInterval, 2, 5)
	stats = tc.RegionStats(statistics.WriteFlow)

	c.Assert(len(stats[1]), Equals, 2)
	c.Assert(len(stats[2]), Equals, 2)
	c.Assert(len(stats[3]), Equals, 1)
	c.Assert(len(stats[5]), Equals, 1)
}
