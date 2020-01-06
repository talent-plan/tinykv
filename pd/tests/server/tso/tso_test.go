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

package tso_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/pd/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
}

func (s *testTsoSuite) SetUpSuite(c *C) {
}

func (s *testTsoSuite) testGetTimestamp(c *C, n int) *pdpb.Timestamp {
	var err error
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())

	clusterID := leaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  uint32(n),
	}

	tsoClient, err := grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))

	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))

	return res
}

func (s *testTsoSuite) TestTso(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}

			for j := 0; j < 30; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(ts.GetPhysical(), Not(Less), last.GetPhysical())
				if ts.GetPhysical() == last.GetPhysical() {
					c.Assert(ts.GetLogical(), Greater, last.GetLogical())
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func (s *testTsoSuite) TestTsoCount0(c *C) {
	var err error
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	req := &pdpb.TsoRequest{Header: testutil.NewRequestHeader(clusterID)}
	tsoClient, err := grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)
}

var _ = Suite(&testTimeFallBackSuite{})

type testTimeFallBackSuite struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cluster      *tests.TestCluster
	grpcPDClient pdpb.PDClient
	server       *tests.TestServer
}

func (s *testTimeFallBackSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	c.Assert(failpoint.Enable("github.com/pingcap-incubator/tinykv/pd/server/tso/fallBackSync", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap-incubator/tinykv/pd/server/tso/fallBackUpdate", `return(true)`), IsNil)
	var err error
	s.cluster, err = tests.NewTestCluster(1)
	c.Assert(err, IsNil)

	err = s.cluster.RunInitialServers()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()

	s.server = s.cluster.GetServer(s.cluster.GetLeader())
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.server.GetAddr())
	svr := s.server.GetServer()
	svr.Close()
	failpoint.Disable("github.com/pingcap-incubator/tinykv/pd/server/tso/fallBackSync")
	failpoint.Disable("github.com/pingcap-incubator/tinykv/pd/server/tso/fallBackUpdate")
	err = svr.Run(s.ctx)
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()
}

func (s *testTimeFallBackSuite) TearDownSuite(c *C) {
	s.cancel()
	s.cluster.Destroy()
}

func (s *testTimeFallBackSuite) testGetTimestamp(c *C, n int) *pdpb.Timestamp {
	clusterID := s.server.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  uint32(n),
	}

	tsoClient, err := s.grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))

	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))
	c.Assert(res.GetPhysical(), Greater, time.Now().UnixNano()/int64(time.Millisecond))

	return res
}

func (s *testTimeFallBackSuite) TestTimeFallBack(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}

			for j := 0; j < 30; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(ts.GetPhysical(), Not(Less), last.GetPhysical())
				if ts.GetPhysical() == last.GetPhysical() {
					c.Assert(ts.GetLogical(), Greater, last.GetLogical())
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}
