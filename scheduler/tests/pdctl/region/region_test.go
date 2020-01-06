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

package region_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/api"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	"github.com/pingcap-incubator/tinykv/scheduler/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&regionTestSuite{})

type regionTestSuite struct{}

func (s *regionTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *regionTestSuite) TestRegionKeyFormat(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	url := cluster.GetConfig().GetClientURLs()
	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)

	echo := pdctl.GetEcho([]string{"-u", url, "region", "key", "--format=raw", " "})
	c.Assert(strings.Contains(string(echo), "unknown flag"), IsFalse)
}

func (s *regionTestSuite) TestRegion(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)

	downPeer := &metapb.Peer{Id: 8, StoreId: 3}
	r1 := pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10),
		core.SetPeers([]*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 5, StoreId: 2},
			{Id: 6, StoreId: 3},
			{Id: 7, StoreId: 4},
		}))
	r2 := pdctl.MustPutRegion(c, cluster, 2, 1, []byte("b"), []byte("c"),
		core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3), core.SetApproximateSize(20))
	r3 := pdctl.MustPutRegion(c, cluster, 3, 1, []byte("c"), []byte("d"),
		core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2), core.SetApproximateSize(30),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}))
	r4 := pdctl.MustPutRegion(c, cluster, 4, 1, []byte("d"), []byte("e"),
		core.SetWrittenBytes(100), core.SetReadBytes(100), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10))
	defer cluster.Destroy()

	// region command
	args := []string{"-u", pdAddr, "region"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo := api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions := leaderServer.GetRegions()
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region <region_id> command
	args = []string{"-u", pdAddr, "region", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo := api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	region := leaderServer.GetRegionInfoByID(1)
	c.Assert(api.NewRegionInfo(region), DeepEquals, &regionInfo)

	// region sibling <region_id> command
	args = []string{"-u", pdAddr, "region", "sibling", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	region = leaderServer.GetRegionInfoByID(2)
	regions = leaderServer.GetAdjacentRegions(region)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region store <store_id> command
	args = []string{"-u", pdAddr, "region", "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = leaderServer.GetStoreRegions(1)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region topread [limit] command
	args = []string{"-u", pdAddr, "region", "topread", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() }, 2)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region topwrite [limit] command
	args = []string{"-u", pdAddr, "region", "topwrite", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, 2)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region topconfver [limit] command
	args = []string{"-u", pdAddr, "region", "topconfver", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	}, 2)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region topversion [limit] command
	args = []string{"-u", pdAddr, "region", "topversion", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	}, 2)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region topsize [limit] command
	args = []string{"-u", pdAddr, "region", "topsize", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	}, 2)
	pdctl.CheckRegionsInfo(c, regionsInfo, regions)

	// region check extra-peer command
	args = []string{"-u", pdAddr, "region", "check", "extra-peer"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r1})

	// region check miss-peer command
	args = []string{"-u", pdAddr, "region", "check", "miss-peer"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r2, r3, r4})

	// region check pending-peer command
	args = []string{"-u", pdAddr, "region", "check", "pending-peer"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3})

	// region check down-peer command
	args = []string{"-u", pdAddr, "region", "check", "down-peer"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3})

	// region key --format=raw <key> command
	args = []string{"-u", pdAddr, "region", "key", "--format=raw", "b"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo = api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	c.Assert(&regionInfo, DeepEquals, api.NewRegionInfo(r2))

	// region key --format=hex <key> command
	args = []string{"-u", pdAddr, "region", "key", "--format=hex", "62"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo = api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	c.Assert(&regionInfo, DeepEquals, api.NewRegionInfo(r2))

	// region startkey --format=raw <key> command
	args = []string{"-u", pdAddr, "region", "startkey", "--format=raw", "b", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r2, r3})

	// region startkey --format=hex <key> command
	args = []string{"-u", pdAddr, "region", "startkey", "--format=hex", "63", "2"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	pdctl.CheckRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3, r4})
}
