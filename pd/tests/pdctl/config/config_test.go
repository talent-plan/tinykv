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

package config_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pingcap-incubator/tinykv/pd/tests"
	"github.com/pingcap-incubator/tinykv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func (s *configTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *configTestSuite) TestConfig(c *C) {
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
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	scheduleCfg := config.ScheduleConfig{}
	cfg := config.Config{}
	cfg.Adjust(nil)
	c.Assert(json.Unmarshal(output, &cfg), IsNil)
	c.Assert(&cfg.Schedule, DeepEquals, svr.GetScheduleConfig())
	c.Assert(&cfg.Replication, DeepEquals, svr.GetReplicationConfig())

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	replicationCfg := config.ReplicationConfig{}
	c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
	c.Assert(&replicationCfg, DeepEquals, svr.GetReplicationConfig())

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion := semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(clusterVersion, Not(DeepEquals), svr.GetClusterVersion())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion = semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg := config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config delete label-property <type> <key> <value>
	args3 := []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set <option> <value>
	args1 = []string{"-u", pdAddr, "config", "set", "leader-schedule-limit", "64"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-schedule-limit", "64"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "5"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	cfg = config.Config{}
	c.Assert(json.Unmarshal(output, &cfg), IsNil)
	scheduleCfg = cfg.Schedule
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, svr.GetScheduleConfig().LeaderScheduleLimit)
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, svr.GetScheduleConfig().HotRegionScheduleLimit)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, svr.GetScheduleConfig().HotRegionCacheHitsThreshold)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, uint64(5))
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, uint64(64))
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, uint64(64))
	args1 = []string{"-u", pdAddr, "config", "set", "enable-remove-down-replica", "false"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	cfg = config.Config{}
	c.Assert(json.Unmarshal(output, &cfg), IsNil)
	scheduleCfg = cfg.Schedule
	c.Assert(scheduleCfg.EnableRemoveDownReplica, Equals, svr.GetScheduleConfig().EnableRemoveDownReplica)
	c.Assert(scheduleCfg.EnableRemoveDownReplica, IsFalse)
	args1 = []string{"-u", pdAddr, "config", "set", "foo-bar", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "config item not found"), IsTrue)
	args1 = []string{"-u", pdAddr, "config", "set", "disable-remove-down-replica", "true"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "already been deprecated"), IsTrue)
}
