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
	"encoding/json"
	"io/ioutil"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
)

var _ = Suite(&testStatusAPISuite{})

type testStatusAPISuite struct {
	hc *http.Client
}

func (s *testStatusAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func checkStatusResponse(c *C, body []byte, cfgs []*config.Config) {
	got := status{}
	c.Assert(json.Unmarshal(body, &got), IsNil)

	c.Assert(got.BuildTS, Equals, server.PDBuildTS)
	c.Assert(got.GitHash, Equals, server.PDGitHash)
}

func (s *testStatusAPISuite) TestStatus(c *C) {
	cfgs, _, clean := mustNewCluster(c, 3)
	defer clean()

	for _, cfg := range cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/status"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkStatusResponse(c, buf, cfgs)
	}
}
