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

package pd

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/mock/mockid"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/testutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	server.EnableZap = true
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	id, _ := i.allocator.Alloc()
	return id
}

var (
	regionIDAllocator = &idAllocator{allocator: &mockid.IDAllocator{}}
	// Note: IDs below are entirely arbitrary. They are only for checking
	// whether GetRegion/GetStore works.
	// If we alloc ID in client in the future, these IDs must be updated.
	stores = []*metapb.Store{
		{Id: 1,
			Address: "localhost:1",
		},
		{Id: 2,
			Address: "localhost:2",
		},
		{Id: 3,
			Address: "localhost:3",
		},
		{Id: 4,
			Address: "localhost:4",
		},
	}

	peers = []*metapb.Peer{
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[0].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[1].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[2].GetId(),
		},
	}
)

type testClientSuite struct {
	cleanup             server.CleanupFunc
	srv                 *server.Server
	client              Client
	grpcSchedulerClient schedulerpb.SchedulerClient
	regionHeartbeat     schedulerpb.Scheduler_RegionHeartbeatClient
}

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	s.srv, s.cleanup, err = server.NewTestServer(c)
	c.Assert(err, IsNil)
	s.grpcSchedulerClient = testutil.MustNewGrpcClient(c, s.srv.GetAddr())

	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), s.grpcSchedulerClient)

	s.client, err = NewClient(s.srv.GetEndpoints(), SecurityOption{})
	c.Assert(err, IsNil)
	s.regionHeartbeat, err = s.grpcSchedulerClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	for _, store := range stores {
		s.srv.PutStore(context.Background(), &schedulerpb.PutStoreRequest{Header: newHeader(s.srv), Store: store})
	}
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.client.Close()
	s.cleanup()
}

func mustWaitLeader(c *C, svrs map[string]*server.Server) *server.Server {
	for i := 0; i < 500; i++ {
		for _, s := range svrs {
			if !s.IsClosed() && s.GetMember().IsLeader() {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func newHeader(srv *server.Server) *schedulerpb.RequestHeader {
	return &schedulerpb.RequestHeader{
		ClusterId: srv.ClusterID(),
	}
}

func bootstrapServer(c *C, header *schedulerpb.RequestHeader, client schedulerpb.SchedulerClient) {
	req := &schedulerpb.BootstrapRequest{
		Header: header,
		Store:  stores[0],
	}
	_, err := client.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TestTSO(c *C) {
	var tss []int64
	for i := 0; i < 100; i++ {
		p, l, err := s.client.GetTS(context.Background())
		c.Assert(err, IsNil)
		tss = append(tss, p<<18+l)
	}

	var last int64
	for _, ts := range tss {
		c.Assert(ts, Greater, last)
		last = ts
	}
}

func (s *testClientSuite) TestTSORace(c *C) {
	var wg sync.WaitGroup
	begin := make(chan struct{})
	count := 10
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			<-begin
			for i := 0; i < 100; i++ {
				_, _, err := s.client.GetTS(context.Background())
				c.Assert(err, IsNil)
			}
			wg.Done()
		}()
	}
	close(begin)
	wg.Wait()
}

func (s *testClientSuite) TestGetRegion(c *C) {
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &schedulerpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peers[0],
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)

	testutil.WaitUntil(c, func(c *C) bool {
		r, leader, err := s.client.GetRegion(context.Background(), []byte("a"))
		c.Assert(err, IsNil)
		return c.Check(r, DeepEquals, region) &&
			c.Check(leader, DeepEquals, peers[0])
	})
	c.Succeed()
}

func (s *testClientSuite) TestGetPrevRegion(c *C) {
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &schedulerpb.RegionHeartbeatRequest{
			Header: newHeader(s.srv),
			Region: r,
			Leader: peers[0],
		}
		err := s.regionHeartbeat.Send(req)
		c.Assert(err, IsNil)
	}
	for i := 0; i < 20; i++ {
		testutil.WaitUntil(c, func(c *C) bool {
			r, leader, err := s.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			c.Assert(err, IsNil)
			if i > 0 && i < regionLen {
				return c.Check(leader, DeepEquals, peers[0]) &&
					c.Check(r, DeepEquals, regions[i-1])
			}
			return c.Check(leader, IsNil) &&
				c.Check(r, IsNil)
		})
	}
	c.Succeed()
}

func (s *testClientSuite) TestScanRegions(c *C) {
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &schedulerpb.RegionHeartbeatRequest{
			Header: newHeader(s.srv),
			Region: r,
			Leader: peers[0],
		}
		err := s.regionHeartbeat.Send(req)
		c.Assert(err, IsNil)
	}

	// Wait for region heartbeats.
	testutil.WaitUntil(c, func(c *C) bool {
		scanRegions, _, err := s.client.ScanRegions(context.Background(), []byte{0}, nil, 10)
		return err == nil && len(scanRegions) == 10
	})

	// Set leader of region3 to nil.
	region3 := core.NewRegionInfo(regions[3], nil)
	s.srv.GetRaftCluster().HandleRegionHeartbeat(region3)

	check := func(start, end []byte, limit int, expect []*metapb.Region) {
		scanRegions, leaders, err := s.client.ScanRegions(context.Background(), start, end, limit)
		c.Assert(err, IsNil)
		c.Assert(scanRegions, HasLen, len(expect))
		c.Assert(leaders, HasLen, len(expect))
		c.Log("scanRegions", scanRegions)
		c.Log("expect", expect)
		c.Log("scanLeaders", leaders)
		for i := range expect {
			c.Assert(scanRegions[i], DeepEquals, expect[i])
			if scanRegions[i].GetId() == region3.GetID() {
				c.Assert(leaders[i], DeepEquals, &metapb.Peer{})
			} else {
				c.Assert(leaders[i], DeepEquals, expect[i].Peers[0])
			}
		}
	}

	check([]byte{0}, nil, 10, regions)
	check([]byte{1}, nil, 5, regions[1:6])
	check([]byte{100}, nil, 1, nil)
	check([]byte{1}, []byte{6}, 0, regions[1:6])
	check([]byte{1}, []byte{6}, 2, regions[1:3])
}

func (s *testClientSuite) TestGetRegionByID(c *C) {
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &schedulerpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peers[0],
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)

	testutil.WaitUntil(c, func(c *C) bool {
		r, leader, err := s.client.GetRegionByID(context.Background(), regionID)
		c.Assert(err, IsNil)
		return c.Check(r, DeepEquals, region) &&
			c.Check(leader, DeepEquals, peers[0])
	})
	c.Succeed()
}

func (s *testClientSuite) TestGetStore(c *C) {
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	store := stores[0]

	// Get an up store should be OK.
	n, err := s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, store)

	stores, err := s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	c.Assert(stores, DeepEquals, stores)

	// Mark the store as offline.
	err = cluster.RemoveStore(store.GetId())
	c.Assert(err, IsNil)
	offlineStore := proto.Clone(store).(*metapb.Store)
	offlineStore.State = metapb.StoreState_Offline

	// Get an offline store should be OK.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, offlineStore)

	// Should return offline stores.
	contains := false
	stores, err = s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == offlineStore.GetId() {
			contains = true
			c.Assert(store, DeepEquals, offlineStore)
		}
	}
	c.Assert(contains, IsTrue)

	// Mark the store as tombstone.
	err = cluster.BuryStore(store.GetId(), true)
	c.Assert(err, IsNil)
	tombstoneStore := proto.Clone(store).(*metapb.Store)
	tombstoneStore.State = metapb.StoreState_Tombstone

	// Get a tombstone store should fail.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, IsNil)

	// Should return tombstone stores.
	contains = false
	stores, err = s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == tombstoneStore.GetId() {
			contains = true
			c.Assert(store, DeepEquals, tombstoneStore)
		}
	}
	c.Assert(contains, IsTrue)

	// Should not return tombstone stores.
	stores, err = s.client.GetAllStores(context.Background(), WithExcludeTombstone())
	c.Assert(err, IsNil)
	for _, store := range stores {
		c.Assert(store, Not(Equals), tombstoneStore)
	}
}

func (s *testClientSuite) checkGCSafePoint(c *C, expectedSafePoint uint64) {
	req := &schedulerpb.GetGCSafePointRequest{
		Header: newHeader(s.srv),
	}
	resp, err := s.srv.GetGCSafePoint(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.SafePoint, Equals, expectedSafePoint)
}

func (s *testClientSuite) TestUpdateGCSafePoint(c *C) {
	s.checkGCSafePoint(c, 0)
	for _, safePoint := range []uint64{0, 1, 2, 3, 233, 23333, 233333333333, math.MaxUint64} {
		newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), safePoint)
		c.Assert(err, IsNil)
		c.Assert(newSafePoint, Equals, safePoint)
		s.checkGCSafePoint(c, safePoint)
	}
	// If the new safe point is less than the old one, it should not be updated.
	newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), 1)
	c.Assert(newSafePoint, Equals, uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	s.checkGCSafePoint(c, math.MaxUint64)
}
