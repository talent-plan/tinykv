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

package health_test

import (
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/api"
	"github.com/pingcap-incubator/tinykv/pd/tests"
	"github.com/pingcap-incubator/tinykv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&healthTestSuite{})

type healthTestSuite struct{}

func (s *healthTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *healthTestSuite) TestHealth(c *C) {
	cluster, err := tests.NewTestCluster(3)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()
	defer cluster.Destroy()

	client := cluster.GetEtcdClient()
	members, err := server.GetMembers(client)
	c.Assert(err, IsNil)
	unhealthMembers := cluster.CheckHealth(members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     true,
		}
		if _, ok := unhealthMembers[member.GetMemberId()]; ok {
			h.Health = false
		}
		healths = append(healths, h)
	}

	// health command
	args := []string{"-u", pdAddr, "health"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	h := make([]api.Health, len(healths))
	c.Assert(json.Unmarshal(output, &h), IsNil)
	c.Assert(err, IsNil)
	c.Assert(h, DeepEquals, healths)
}
