// Copyright 2016 PingCAP, Inc.
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

package id_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

const allocStep = uint64(1000)

var _ = Suite(&testAllocIDSuite{})

type testAllocIDSuite struct{}

func (s *testAllocIDSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *testAllocIDSuite) TestID(c *C) {
	var err error
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	var last uint64
	for i := uint64(0); i < allocStep; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}

	var wg sync.WaitGroup

	var m sync.Mutex
	ids := make(map[uint64]struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				id, err := leaderServer.GetAllocator().Alloc()
				c.Assert(err, IsNil)
				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()
}

func (s *testAllocIDSuite) TestCommand(c *C) {
	var err error
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	req := &schedulerpb.AllocIDRequest{
		Header: testutil.NewRequestHeader(leaderServer.GetClusterID()),
	}

	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	var last uint64
	for i := uint64(0); i < 2*allocStep; i++ {
		resp, err := grpcPDClient.AllocID(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetId(), Greater, last)
		last = resp.GetId()
	}
}
