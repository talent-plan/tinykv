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

package api

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
)

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	hc      *http.Client
	cfgs    []*config.Config
	servers []*server.Server
	clean   func()
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
	s.cfgs, s.servers, s.clean = mustNewCluster(c, 3)
}

func (s *testMemberAPISuite) TearDownSuite(c *C) {
	s.clean()
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cfgs []*config.Config) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cfgs))

	for _, memb := range got["members"] {
		for _, cfg := range cfgs {
			if memb.GetName() != cfg.Name {
				continue
			}

			relaxEqualStings(c, memb.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(c, memb.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	for _, cfg := range s.cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/members"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, s.cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberLeader(c *C) {
	leader := s.servers[0].GetLeader()
	addr := s.cfgs[rand.Intn(len(s.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	var got pdpb.Member
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	c.Assert(got.GetClientUrls(), DeepEquals, leader.GetClientUrls())
	c.Assert(got.GetMemberId(), Equals, leader.GetMemberId())
}
