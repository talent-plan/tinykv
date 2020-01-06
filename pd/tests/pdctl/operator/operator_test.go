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

package operator_test

import (
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pingcap-incubator/tinykv/pd/tests"
	"github.com/pingcap-incubator/tinykv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&operatorTestSuite{})

type operatorTestSuite struct{}

func (s *operatorTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *operatorTestSuite) TestOperator(c *C) {
	var err error
	var t time.Time
	t = t.Add(time.Hour)
	cluster, err := tests.NewTestCluster(1,
		func(conf *config.Config) { conf.Replication.MaxReplicas = 2 },
		func(conf *config.Config) { conf.Schedule.MaxStoreDownTime.Duration = time.Since(t) },
		func(conf *config.Config) { conf.Schedule.StoreBalanceRate = 240 },
	)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}

	pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"), core.SetPeers([]*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
	}))
	pdctl.MustPutRegion(c, cluster, 3, 2, []byte("b"), []byte("c"), core.SetPeers([]*metapb.Peer{
		{Id: 3, StoreId: 1},
		{Id: 4, StoreId: 2},
	}))
	defer cluster.Destroy()

	var testCases = []struct {
		cmd    []string
		show   []string
		expect string
		reset  []string
	}{
		{
			// operator add add-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "add-peer", "1", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "promote learner peer 1 on store 3",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add remove-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "remove-peer", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-leader <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-leader", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show", "leader"},
			expect: "transfer leader from store 1 to store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-region <region_id> <to_store_id>...
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-region", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show", "region"},
			expect: "remove peer on store 1",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-peer <region_id> <from_store_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-peer", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy APPROXIMATE",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "status: RUNNING",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
	}

	for _, testCase := range testCases {
		_, _, e := pdctl.ExecuteCommandC(cmd, testCase.cmd...)
		c.Assert(e, IsNil)
		_, output, e := pdctl.ExecuteCommandC(cmd, testCase.show...)
		c.Assert(e, IsNil)
		c.Assert(strings.Contains(string(output), testCase.expect), IsTrue)
		_, _, e = pdctl.ExecuteCommandC(cmd, testCase.reset...)
		c.Assert(e, IsNil)
	}

	// operator add merge-region <source_region_id> <target_region_id>
	args := []string{"-u", pdAddr, "operator", "add", "merge-region", "1", "3"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "merge region 1 into region 3"), IsTrue)
	args = []string{"-u", pdAddr, "operator", "remove", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "remove", "3"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// operator add scatter-region <region_id>
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "3"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "show", "region"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "scatter-region"), IsTrue)

	// test echo
	echo := pdctl.GetEcho([]string{"-u", pdAddr, "operator", "add", "scatter-region", "1"})
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = pdctl.GetEcho([]string{"-u", pdAddr, "operator", "remove", "1"})
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = pdctl.GetEcho([]string{"-u", pdAddr, "operator", "remove", "1"})
	c.Assert(strings.Contains(echo, "Success!"), IsFalse)
}
