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

package server_test

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tempurl"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	. "github.com/pingcap/check"

	// Register schedulers.
	_ "github.com/pingcap-incubator/tinykv/scheduler/server/schedulers"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct{}

func (s *serverTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *serverTestSuite) TestUpdateAdvertiseUrls(c *C) {
	cluster, err := tests.NewTestCluster(2)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// AdvertisePeerUrls should equals to PeerUrls.
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		c.Assert(serverConf.AdvertisePeerUrls, Equals, conf.PeerURLs)
		c.Assert(serverConf.AdvertiseClientUrls, Equals, conf.ClientURLs)
	}

	err = cluster.StopAll()
	c.Assert(err, IsNil)

	// Change config will not affect peer urls.
	// Recreate servers with new peer URLs.
	for _, conf := range cluster.GetConfig().InitialServers {
		conf.AdvertisePeerURLs = conf.PeerURLs + "," + tempurl.Alloc()
	}
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf, e := conf.Generate()
		c.Assert(e, IsNil)
		s, e := tests.NewTestServer(serverConf)
		c.Assert(e, IsNil)
		cluster.GetServers()[conf.Name] = s
	}
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		c.Assert(serverConf.AdvertisePeerUrls, Equals, conf.PeerURLs)
	}
}

func (s *serverTestSuite) TestClusterID(c *C) {
	cluster, err := tests.NewTestCluster(3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	clusterID := cluster.GetServer("pd1").GetClusterID()
	for _, s := range cluster.GetServers() {
		c.Assert(s.GetClusterID(), Equals, clusterID)
	}

	// Restart all PDs.
	err = cluster.StopAll()
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// All PDs should have the same cluster ID as before.
	for _, s := range cluster.GetServers() {
		c.Assert(s.GetClusterID(), Equals, clusterID)
	}
}

func (s *serverTestSuite) TestLeader(c *C) {
	cluster, err := tests.NewTestCluster(3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	leader1 := cluster.WaitLeader()
	c.Assert(leader1, Not(Equals), "")

	err = cluster.GetServer(leader1).Stop()
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		leader := cluster.GetLeader()
		return leader != leader1
	})
}
