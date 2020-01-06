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

package compatibility_test

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&compatibilityTestSuite{})

type compatibilityTestSuite struct{}

func (s *compatibilityTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *compatibilityTestSuite) TestStoreRegister(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	putStoreRequest := &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      1,
			Address: "mock-1",
			Version: "2.0.1",
		},
	}

	svr := leaderServer.GetServer()
	_, err = svr.PutStore(context.Background(), putStoreRequest)
	c.Assert(err, IsNil)
	// FIX ME: read v0.0.0 in sometime
	cluster.WaitLeader()
	version := leaderServer.GetClusterVersion()
	// Restart all PDs.
	err = cluster.StopAll()
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer, NotNil)
	newVersion := leaderServer.GetClusterVersion()
	c.Assert(version, Equals, newVersion)

	// putNewStore with old version
	putStoreRequest = &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      4,
			Address: "mock-4",
			Version: "1.0.1",
		},
	}
	_, err = svr.PutStore(context.Background(), putStoreRequest)
	c.Assert(err, NotNil)
}

func (s *compatibilityTestSuite) TestRollingUpgrade(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	stores := []*pdpb.PutStoreRequest{
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      1,
				Address: "mock-1",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      4,
				Address: "mock-4",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      6,
				Address: "mock-6",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      7,
				Address: "mock-7",
				Version: "2.0.1",
			},
		},
	}

	svr := leaderServer.GetServer()
	for _, store := range stores {
		_, err = svr.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
	}
	c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 0, Patch: 1})
	// rolling update
	for i, store := range stores {
		if i == 0 {
			store.Store.State = metapb.StoreState_Tombstone
		}
		store.Store.Version = "2.1.0"
		resp, err := svr.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
		if i != len(stores)-1 {
			c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 0, Patch: 1})
			c.Assert(resp.GetHeader().GetError(), IsNil)
		}
	}
	c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 1})
}
