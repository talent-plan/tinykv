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

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	. "github.com/pingcap/check"
)

func TestServer(t *testing.T) {
	EnableZap = true
	TestingT(t)
}

func mustRunTestServer(c *C) (*Server, CleanupFunc) {
	var err error
	server, cleanup, err := NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{server})
	return server, cleanup
}

func mustWaitLeader(c *C, svrs []*Server) *Server {
	var leader *Server
	testutil.WaitUntil(c, func(c *C) bool {
		for _, s := range svrs {
			if !s.IsClosed() && s.member.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	return leader
}

var _ = Suite(&testLeaderServerSuite{})

type testLeaderServerSuite struct {
	ctx        context.Context
	cancel     context.CancelFunc
	svrs       map[string]*Server
	leaderPath string
}

func (s *testLeaderServerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.svrs = make(map[string]*Server)

	cfgs := NewTestMultiConfig(c, 3)

	ch := make(chan *Server, 3)
	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := CreateServer(cfg)
			c.Assert(err, IsNil)
			err = svr.Run(s.ctx)
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < 3; i++ {
		svr := <-ch
		s.svrs[svr.GetAddr()] = svr
		s.leaderPath = svr.GetMember().GetLeaderPath()
	}
}

func (s *testLeaderServerSuite) TearDownSuite(c *C) {
	s.cancel()
	for _, svr := range s.svrs {
		svr.Close()
		testutil.CleanServer(svr.cfg)
	}
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct{}

func newTestServersWithCfgs(ctx context.Context, c *C, cfgs []*config.Config) ([]*Server, CleanupFunc) {
	svrs := make([]*Server, 0, len(cfgs))

	ch := make(chan *Server)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			svr, err := CreateServer(cfg)
			c.Assert(err, IsNil)
			err = svr.Run(ctx)
			c.Assert(err, IsNil)
			ch <- svr
		}(cfg)
	}

	for i := 0; i < len(cfgs); i++ {
		svrs = append(svrs, <-ch)
	}
	mustWaitLeader(c, svrs)

	cleanup := func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg)
		}
	}

	return svrs, cleanup
}

func (s *testServerSuite) TestCheckClusterID(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgs := NewTestMultiConfig(c, 2)
	for i, cfg := range cfgs {
		cfg.DataDir = fmt.Sprintf("/tmp/test_pd_check_clusterID_%d", i)
		// Clean up before testing.
		testutil.CleanServer(cfg)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster
	// TODO: clean up. For now tests failed because:
	//    etcdserver: failed to purge snap file ...
	svrsA, cleanA := newTestServersWithCfgs(ctx, c, []*config.Config{cfgA})
	defer cleanA()
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := newTestServersWithCfgs(ctx, c, []*config.Config{cfgB})
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	svr, err := CreateServer(cfgA)
	c.Assert(err, IsNil)
	err = svr.Run(ctx)
	c.Assert(err, NotNil)
}
