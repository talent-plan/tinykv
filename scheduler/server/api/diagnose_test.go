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

package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
)

var _ = Suite(&testDiagnoseAPISuite{})

type testDiagnoseAPISuite struct {
	hc *http.Client
}

func (s *testDiagnoseAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func checkDiagnoseResponse(c *C, body []byte) {
	got := []Recommendation{}
	c.Assert(json.Unmarshal(body, &got), IsNil)
	for _, r := range got {
		c.Assert(len(r.Module) != 0, IsTrue)
		c.Assert(len(r.Level) != 0, IsTrue)
		c.Assert(len(r.Description) != 0, IsTrue)
		c.Assert(len(r.Instruction) != 0, IsTrue)
	}
}

func (s *testDiagnoseAPISuite) TestDiagnoseSlice(c *C) {
	_, svrs, clean := mustNewCluster(c, 3)
	defer clean()
	var leader, follow *server.Server

	for _, svr := range svrs {
		if !svr.IsClosed() && svr.GetMember().IsLeader() {
			leader = svr
		} else {
			follow = svr
		}
	}
	addr := leader.GetConfig().ClientUrls + apiPrefix + "/diagnose"
	follow.Close()
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	checkDiagnoseResponse(c, buf)
}
