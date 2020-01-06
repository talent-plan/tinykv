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

package label_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/api"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pingcap-incubator/tinykv/pd/tests"
	"github.com/pingcap-incubator/tinykv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&labelTestSuite{})

type labelTestSuite struct{}

func (s *labelTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *labelTestSuite) TestLabel(c *C) {
	cluster, err := tests.NewTestCluster(1, func(cfg *config.Config) { cfg.Replication.StrictlyMatchLabel = false })
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv2",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-east",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      3,
			Address: "tikv3",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// label command
	args := []string{"-u", pdAddr, "label"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	labels := make([]*metapb.StoreLabel, 0, len(stores))
	c.Assert(json.Unmarshal(output, &labels), IsNil)
	got := make(map[string]struct{})
	for _, l := range labels {
		if _, ok := got[strings.ToLower(l.Key+l.Value)]; !ok {
			got[strings.ToLower(l.Key+l.Value)] = struct{}{}
		}
	}
	expected := make(map[string]struct{})
	ss := leaderServer.GetStores()
	for _, s := range ss {
		ls := s.GetLabels()
		for _, l := range ls {
			if _, ok := expected[strings.ToLower(l.Key+l.Value)]; !ok {
				expected[strings.ToLower(l.Key+l.Value)] = struct{}{}
			}
		}
	}
	c.Assert(got, DeepEquals, expected)

	// label store <name> command
	args = []string{"-u", pdAddr, "label", "store", "zone", "us-west"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	ss = []*metapb.Store{stores[0], stores[2]}
	pdctl.CheckStoresInfo(c, storesInfo.Stores, ss)
}
