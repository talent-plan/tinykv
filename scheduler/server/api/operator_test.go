// Copyright 2017 PingCAP, Inc.
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

package api

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
)

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testOperatorSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) { cfg.Replication.MaxReplicas = 1 })
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testOperatorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testOperatorSuite) TestAddRemovePeer(c *C) {
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, nil)

	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	regionInfo := core.NewRegionInfo(region, peer1)
	mustRegionHeartbeat(c, s.svr, regionInfo)

	regionURL := fmt.Sprintf("%s/operators/%d", s.urlPrefix, region.GetId())
	operator := mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "operator not found"), IsTrue)

	mustPutStore(c, s.svr, 3, metapb.StoreState_Up, nil)
	err := postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 1 on store 3"), IsTrue)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)

	err = doDelete(regionURL)
	c.Assert(err, IsNil)

	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)
	c.Assert(strings.Contains(operator, "remove peer on store 2"), IsTrue)

	err = doDelete(regionURL)
	c.Assert(err, IsNil)

	mustPutStore(c, s.svr, 4, metapb.StoreState_Up, nil)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 2 on store 4"), IsTrue)

	// Fail to add peer to tombstone store.
	err = s.svr.GetRaftCluster().BuryStore(3, true)
	c.Assert(err, IsNil)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, NotNil)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`))
	c.Assert(err, NotNil)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`))
	c.Assert(err, NotNil)
}

func (s *testOperatorSuite) TestMergeRegionOperator(c *C) {
	r1 := newTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(c, s.svr, r1)
	r2 := newTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(c, s.svr, r2)
	r3 := newTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(c, s.svr, r3)

	err := postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`))
	c.Assert(err, IsNil)

	s.svr.GetHandler().RemoveOperator(10)
	s.svr.GetHandler().RemoveOperator(20)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 20, "target_region_id": 10}`))
	c.Assert(err, IsNil)
	s.svr.GetHandler().RemoveOperator(10)
	s.svr.GetHandler().RemoveOperator(20)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 30}`))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not adjacent"), IsTrue)
	err = postJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 30, "target_region_id": 10}`))

	c.Assert(strings.Contains(err.Error(), "not adjacent"), IsTrue)
	c.Assert(err, NotNil)
}

func mustPutStore(c *C, svr *server.Server, id uint64, state metapb.StoreState, labels []*metapb.StoreLabel) {
	_, err := svr.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:      id,
			Address: fmt.Sprintf("tikv%d", id),
			State:   state,
			Labels:  labels,
			Version: (*server.MinSupportedVersion(server.Version2_0)).String(),
		},
	})
	c.Assert(err, IsNil)
}

func mustRegionHeartbeat(c *C, svr *server.Server, region *core.RegionInfo) {
	cluster := svr.GetRaftCluster()
	err := cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)
}

func mustReadURL(c *C, url string) string {
	res, err := http.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	return string(data)
}
