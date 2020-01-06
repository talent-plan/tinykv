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

package syncer_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockid"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct{}

func (s *serverTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	v, _ := i.allocator.Alloc()
	return v
}

func (s *serverTestSuite) TestRegionSyncer(c *C) {
	cluster, err := tests.NewTestCluster(3, func(conf *config.Config) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)
	regionLen := 110
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    []*metapb.Peer{{Id: allocator.alloc(), StoreId: uint64(0)}},
		}
		regions = append(regions, core.NewRegionInfo(r, r.Peers[0]))
	}
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	// ensure flush to region storage, we use a duration larger than the
	// region storage flush rate limit (3s).
	time.Sleep(4 * time.Second)
	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer, NotNil)
	loadRegions := leaderServer.GetServer().GetRaftCluster().GetRegions()
	c.Assert(len(loadRegions), Equals, regionLen)
}

func (s *serverTestSuite) TestFullSyncWithAddMember(c *C) {
	cluster, err := tests.NewTestCluster(1, func(conf *config.Config) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)
	regionLen := 110
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    []*metapb.Peer{{Id: allocator.alloc(), StoreId: uint64(0)}},
		}
		regions = append(regions, core.NewRegionInfo(r, r.Peers[0]))
	}
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	// restart pd1
	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	err = leaderServer.Run(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd1")

	// join new PD
	pd2, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd2.Run(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd1")
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd2")
	loadRegions := pd2.GetServer().GetRaftCluster().GetRegions()
	c.Assert(len(loadRegions), Equals, regionLen)
}
