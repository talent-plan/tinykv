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

package statistics

import (
	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/pkg/mock/mockoption"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
)

var _ = Suite(&testRegionStatisticsSuite{})

type testRegionStatisticsSuite struct{}

func (t *testRegionStatisticsSuite) TestRegionStatistics(c *C) {
	opt := mockoption.NewScheduleOptions()
	peers := []*metapb.Peer{
		{Id: 5, StoreId: 1},
		{Id: 6, StoreId: 2},
		{Id: 4, StoreId: 3},
		{Id: 8, StoreId: 7, IsLearner: true},
	}

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1"},
		{Id: 2, Address: "mock://tikv-2"},
		{Id: 3, Address: "mock://tikv-3"},
		{Id: 7, Address: "mock://tikv-7"},
	}
	var stores []*core.StoreInfo
	for _, m := range metaStores {
		s := core.NewStoreInfo(m)
		stores = append(stores, s)
	}

	downPeers := []*pdpb.PeerStats{
		{Peer: peers[0], DownSeconds: 3608},
		{Peer: peers[1], DownSeconds: 3608},
	}

	store3 := stores[3].Clone(core.SetStoreState(metapb.StoreState_Offline))
	stores[3] = store3
	r1 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("aa"), EndKey: []byte("bb")}
	r2 := &metapb.Region{Id: 2, Peers: peers[0:2], StartKey: []byte("cc"), EndKey: []byte("dd")}
	region1 := core.NewRegionInfo(r1, peers[0])
	region2 := core.NewRegionInfo(r2, peers[0])
	regionStats := NewRegionStatistics(opt)
	regionStats.Observe(region1, stores)
	c.Assert(len(regionStats.stats[ExtraPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[LearnerPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[EmptyRegion]), Equals, 1)

	region1 = region1.Clone(
		core.WithDownPeers(downPeers),
		core.WithPendingPeers(peers[0:1]),
		core.SetApproximateSize(144),
	)
	regionStats.Observe(region1, stores)
	c.Assert(len(regionStats.stats[ExtraPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[MissPeer]), Equals, 0)
	c.Assert(len(regionStats.stats[DownPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[PendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[LearnerPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[EmptyRegion]), Equals, 0)

	region2 = region2.Clone(core.WithDownPeers(downPeers[0:1]))
	regionStats.Observe(region2, stores[0:2])
	c.Assert(len(regionStats.stats[ExtraPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[MissPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[DownPeer]), Equals, 2)
	c.Assert(len(regionStats.stats[PendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[LearnerPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[OfflinePeer]), Equals, 1)

	region1 = region1.Clone(core.WithRemoveStorePeer(7))
	regionStats.Observe(region1, stores[0:3])
	c.Assert(len(regionStats.stats[ExtraPeer]), Equals, 0)
	c.Assert(len(regionStats.stats[MissPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[DownPeer]), Equals, 2)
	c.Assert(len(regionStats.stats[PendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[LearnerPeer]), Equals, 0)
	c.Assert(len(regionStats.stats[OfflinePeer]), Equals, 0)

	store3 = stores[3].Clone(core.SetStoreState(metapb.StoreState_Up))
	stores[3] = store3
	regionStats.Observe(region1, stores)
	c.Assert(len(regionStats.stats[OfflinePeer]), Equals, 0)
}

func (t *testRegionStatisticsSuite) TestRegionLabelIsolationLevel(c *C) {
	locationLabels := []string{"zone", "rack", "host"}
	labelLevelStats := NewLabelStatistics()
	labelsSet := [][]map[string]string{
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r1", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by host when location labels is ["zone", "rack", "host"]
			// cannot be isolated when location labels is ["zone", "rack"]
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by zone
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z3", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r3", "host": "h3"},
		},
		{
			// cannot be isolated
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
		},
	}
	res := []string{"rack", "host", "zone", "rack", "none"}
	counter := map[string]int{"none": 1, "host": 1, "rack": 2, "zone": 1}
	regionID := 1
	f := func(labels []map[string]string, res string, locationLabels []string) {
		metaStores := []*metapb.Store{
			{Id: 1, Address: "mock://tikv-1"},
			{Id: 2, Address: "mock://tikv-2"},
			{Id: 3, Address: "mock://tikv-3"},
		}
		stores := make([]*core.StoreInfo, 0, len(labels))
		for i, m := range metaStores {
			var newLabels []*metapb.StoreLabel
			for k, v := range labels[i] {
				newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
			}
			s := core.NewStoreInfo(m, core.SetStoreLabels(newLabels))

			stores = append(stores, s)
		}
		region := core.NewRegionInfo(&metapb.Region{Id: uint64(regionID)}, nil)
		label := getRegionLabelIsolation(stores, locationLabels)
		labelLevelStats.Observe(region, stores, locationLabels)
		c.Assert(label, Equals, res)
		regionID++
	}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		c.Assert(labelLevelStats.labelCounter[i], Equals, res)
	}

	label := getRegionLabelIsolation(nil, locationLabels)
	c.Assert(label, Equals, nonIsolation)
	label = getRegionLabelIsolation(nil, nil)
	c.Assert(label, Equals, nonIsolation)

	regionID = 1
	res = []string{"rack", "none", "zone", "rack", "none"}
	counter = map[string]int{"none": 2, "host": 0, "rack": 2, "zone": 1}
	locationLabels = []string{"zone", "rack"}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		c.Assert(labelLevelStats.labelCounter[i], Equals, res)
	}
}
