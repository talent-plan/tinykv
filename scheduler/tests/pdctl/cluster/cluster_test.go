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

package cluster_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	"github.com/pingcap-incubator/tinykv/scheduler/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&clusterTestSuite{})

type clusterTestSuite struct{}

func (s *clusterTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *clusterTestSuite) TestClusterAndPing(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	i := strings.Index(pdAddr, "//")
	pdAddr = pdAddr[i+2:]
	cmd := pdctl.InitCommand()
	defer cluster.Destroy()

	// cluster
	args := []string{"-u", pdAddr, "cluster"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci := &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	echo := pdctl.GetEcho([]string{"-u", pdAddr, "--cacert=ca.pem", "cluster"})
	c.Assert(strings.Contains(echo, "no such file or directory"), IsTrue)

	// cluster status
	args = []string{"-u", pdAddr, "cluster", "status"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci = &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	// ping
	args = []string{"-u", pdAddr, "ping"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(output, NotNil)
}
