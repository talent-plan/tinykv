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

package testutil

import (
	"os"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	check "github.com/pingcap/check"
	"google.golang.org/grpc"
)

const (
	waitMaxRetry   = 200
	waitRetrySleep = time.Millisecond * 100
)

// CheckFunc is a condition checker that passed to WaitUntil. Its implementation
// may call c.Fatal() to abort the test, or c.Log() to add more information.
type CheckFunc func(c *check.C) bool

// WaitUntil repeatly evaluates f() for a period of time, util it returns true.
func WaitUntil(c *check.C, f CheckFunc) {
	c.Log("wait start")
	for i := 0; i < waitMaxRetry; i++ {
		if f(c) {
			return
		}
		time.Sleep(waitRetrySleep)
	}
	c.Fatal("wait timeout")
}

// NewRequestHeader creates a new request header.
func NewRequestHeader(clusterID uint64) *schedulerpb.RequestHeader {
	return &schedulerpb.RequestHeader{
		ClusterId: clusterID,
	}
}

// MustNewGrpcClient must create a new grpc client.
func MustNewGrpcClient(c *check.C, addr string) schedulerpb.SchedulerClient {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure())

	c.Assert(err, check.IsNil)
	return schedulerpb.NewSchedulerClient(conn)
}

// CleanServer is used to clean data directory.
func CleanServer(cfg *config.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}
