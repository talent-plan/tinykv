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

package tso_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&tsoTestSuite{})

type tsoTestSuite struct{}

func (s *tsoTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *tsoTestSuite) TestTSO(c *C) {
	cmd := pdctl.InitCommand()

	const (
		physicalShiftBits = 18
		logicalBits       = 0x3FFFF
	)

	// tso command
	ts := "395181938313123110"
	args := []string{"-u", "127.0.0.1", "tso", ts}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	t, e := strconv.ParseUint(ts, 10, 64)
	c.Assert(e, IsNil)
	c.Assert(err, IsNil)
	logicalTime := t & logicalBits
	physical := t >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical%1000)*time.Millisecond.Nanoseconds())
	str := fmt.Sprintln("system: ", physicalTime) + fmt.Sprintln("logic: ", logicalTime)
	c.Assert(str, Equals, string(output))
}
