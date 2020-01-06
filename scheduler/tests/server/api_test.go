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
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
)

func (s *serverTestSuite) TestReconnect(c *C) {
	cluster, err := tests.NewTestCluster(3, func(conf *config.Config) {
		conf.TickInterval = typeutil.Duration{50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := cluster.WaitLeader()
	for name, s := range cluster.GetServers() {
		if name != leader {
			res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
			c.Assert(e, IsNil)
			c.Assert(res.StatusCode, Equals, http.StatusOK)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	newLeader := cluster.WaitLeader()
	c.Assert(len(newLeader), Not(Equals), 0)

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.GetServers() {
		if name != leader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(e, IsNil)
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	err = cluster.GetServer(newLeader).Stop()
	c.Assert(err, IsNil)

	// Request will fail with no leader.
	for name, s := range cluster.GetServers() {
		if name != leader && name != newLeader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(err, IsNil)
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}
