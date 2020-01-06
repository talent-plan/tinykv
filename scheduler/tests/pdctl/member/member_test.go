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

package member_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	"github.com/pingcap-incubator/tinykv/scheduler/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&memberTestSuite{})

type memberTestSuite struct{}

func (s *memberTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *memberTestSuite) TestMember(c *C) {
	cluster, err := tests.NewTestCluster(3)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdAddr := cluster.GetConfig().GetClientURLs()
	c.Assert(err, IsNil)
	cmd := pdctl.InitCommand()
	svr := cluster.GetServer("pd2")
	id := svr.GetServerID()
	name := svr.GetServer().Name()
	client := cluster.GetServer("pd1").GetEtcdClient()
	defer cluster.Destroy()

	// member leader show
	args := []string{"-u", pdAddr, "member", "leader", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	leader := pdpb.Member{}
	c.Assert(json.Unmarshal(output, &leader), IsNil)
	c.Assert(&leader, DeepEquals, svr.GetLeader())

	// member leader transfer <member_name>
	args = []string{"-u", pdAddr, "member", "leader", "transfer", "pd2"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check("pd2", Equals, svr.GetLeader().GetName())
	})

	// member leader resign
	cluster.WaitLeader()
	args = []string{"-u", pdAddr, "member", "leader", "resign"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(strings.Contains(string(output), "Success"), IsTrue)
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check("pd2", Not(Equals), svr.GetLeader().GetName())
	})

	// member leader_priority <member_name> <priority>
	cluster.WaitLeader()
	args = []string{"-u", pdAddr, "member", "leader_priority", name, "100"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	priority, err := svr.GetServer().GetMember().GetMemberLeaderPriority(id)
	c.Assert(err, IsNil)
	c.Assert(priority, Equals, 100)

	// member delete name <member_name>
	err = svr.Destroy()
	c.Assert(err, IsNil)
	members, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 3)
	args = []string{"-u", pdAddr, "member", "delete", "name", name}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 2)

	// member delete id <member_id>
	args = []string{"-u", pdAddr, "member", "delete", "id", string(id)}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 2)
	c.Succeed()
}
