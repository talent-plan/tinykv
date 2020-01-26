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

package client_test

import (
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct{}

func (s *serverTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

type client interface {
	GetLeaderAddr() string
	ScheduleCheckLeader()
	GetURLs() []string
}

func (s *serverTestSuite) TestClientLeaderChange(c *C) {
	cluster, err := tests.NewTestCluster(3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewClient(endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	var p1, l1 int64
	testutil.WaitUntil(c, func(c *C) bool {
		p1, l1, err = cli.GetTS(context.TODO())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})

	leader := cluster.GetLeader()
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	leader = cluster.WaitLeader()
	c.Assert(leader, Not(Equals), "")
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	// Check TS won't fall back after leader changed.
	testutil.WaitUntil(c, func(c *C) bool {
		p2, l2, err := cli.GetTS(context.TODO())
		if err != nil {
			c.Log(err)
			return false
		}
		c.Assert(p1<<18+l1, Less, p2<<18+l2)
		return true
	})

	// Check URL list.
	cli.Close()
	urls := cli.(client).GetURLs()
	sort.Strings(urls)
	sort.Strings(endpoints)
	c.Assert(urls, DeepEquals, endpoints)
}

func (s *serverTestSuite) TestLeaderTransfer(c *C) {
	cluster, err := tests.NewTestCluster(2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewClient(endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	var physical, logical int64
	testutil.WaitUntil(c, func(c *C) bool {
		physical, logical, err = cli.GetTS(context.TODO())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})
	lastTS := s.makeTS(physical, logical)
	// Start a goroutine the make sure TS won't fall back.
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			default:
			}

			physical, logical, err1 := cli.GetTS(context.TODO())
			if err1 == nil {
				ts := s.makeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
			time.Sleep(time.Millisecond)
		}
	}()
	// Transfer leader.
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	})
	c.Assert(err, IsNil)
	leaderPath := filepath.Join("/pd", strconv.FormatUint(cli.GetClusterID(context.Background()), 10), "leader")
	for i := 0; i < 10; i++ {
		cluster.WaitLeader()
		_, err = etcdCli.Delete(context.TODO(), leaderPath)
		c.Assert(err, IsNil)
		// Sleep to make sure all servers are notified and starts campaign.
		time.Sleep(time.Second)
	}
	close(quit)
	wg.Wait()
}

func (s *serverTestSuite) waitLeader(c *C, cli client, leader string) {
	testutil.WaitUntil(c, func(c *C) bool {
		cli.ScheduleCheckLeader()
		return cli.GetLeaderAddr() == leader
	})
}

func (s *serverTestSuite) makeTS(physical, logical int64) uint64 {
	return uint64(physical<<18 + logical)
}
