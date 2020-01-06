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

package join_test

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/pd/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/join"
	"github.com/pingcap-incubator/tinykv/pd/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&joinTestSuite{})

type joinTestSuite struct{}

func (s *joinTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *joinTestSuite) TestSimpleJoin(c *C) {
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	pd1 := cluster.GetServer("pd1")
	client := pd1.GetEtcdClient()
	members, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(members.Members, HasLen, 1)

	// Join the second PD.
	pd2, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd2.Run(context.TODO())
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(pd2.GetConfig().DataDir, "join"))
	c.Assert(os.IsNotExist(err), IsFalse)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(members.Members, HasLen, 2)
	c.Assert(pd2.GetClusterID(), Equals, pd1.GetClusterID())

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join another PD.
	pd3, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd3.Run(context.TODO())
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(pd3.GetConfig().DataDir, "join"))
	c.Assert(os.IsNotExist(err), IsFalse)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(members.Members, HasLen, 3)
	c.Assert(pd3.GetClusterID(), Equals, pd1.GetClusterID())
}

// A failed PD tries to join the previous cluster but it has been deleted
// during its downtime.
func (s *joinTestSuite) TestFailedAndDeletedPDJoinsPreviousCluster(c *C) {
	cluster, err := tests.NewTestCluster(3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	pd3 := cluster.GetServer("pd3")
	err = pd3.Stop()
	c.Assert(err, IsNil)

	client := cluster.GetServer("pd1").GetEtcdClient()
	_, err = client.MemberRemove(context.TODO(), pd3.GetServerID())
	c.Assert(err, IsNil)

	// The server should not successfully start.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()
	res := cluster.RunServer(ctx, pd3)
	c.Assert(<-res, NotNil)

	members, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(members.Members, HasLen, 2)
}

// A deleted PD joins the previous cluster.
func (s *joinTestSuite) TestDeletedPDJoinsPreviousCluster(c *C) {
	cluster, err := tests.NewTestCluster(3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	pd3 := cluster.GetServer("pd3")
	client := cluster.GetServer("pd1").GetEtcdClient()
	_, err = client.MemberRemove(context.TODO(), pd3.GetServerID())
	c.Assert(err, IsNil)

	err = pd3.Stop()
	c.Assert(err, IsNil)

	// The server should not successfully start.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()
	res := cluster.RunServer(ctx, pd3)
	c.Assert(<-res, NotNil)

	members, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(members.Members, HasLen, 2)
}

func (s *joinTestSuite) TestFailedPDJoinsPreviousCluster(c *C) {
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	// Join the second PD.
	pd2, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd2.Run(context.TODO())
	c.Assert(err, IsNil)
	err = pd2.Stop()
	c.Assert(err, IsNil)
	err = pd2.Destroy()
	c.Assert(err, IsNil)
	c.Assert(join.PrepareJoinCluster(pd2.GetConfig()), NotNil)
}
