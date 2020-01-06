// Copyright 2019 PingCAP, Inc.
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

package join_fail_test

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&joinTestSuite{})

type joinTestSuite struct{}

func (s *joinTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *joinTestSuite) TestFailedPDJoinInStep1(c *C) {
	cluster, err := tests.NewTestCluster(1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	// Join the second PD.
	c.Assert(failpoint.Enable("github.com/pingcap-incubator/tinykv/scheduler/server/join/add-member-failed", `return`), IsNil)
	_, err = cluster.Join()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "join failed"), IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap-incubator/tinykv/scheduler/server/join/add-member-failed"), IsNil)
}
