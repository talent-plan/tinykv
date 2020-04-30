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

package schedulers

import (
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	. "github.com/pingcap/check"
)

func TestSchedulers(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMinMaxSuite{})

type testMinMaxSuite struct{}

func (s *testMinMaxSuite) TestMinUint64(c *C) {
	c.Assert(minUint64(1, 2), Equals, uint64(1))
	c.Assert(minUint64(2, 1), Equals, uint64(1))
	c.Assert(minUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMaxUint64(c *C) {
	c.Assert(maxUint64(1, 2), Equals, uint64(2))
	c.Assert(maxUint64(2, 1), Equals, uint64(2))
	c.Assert(maxUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMinDuration(c *C) {
	c.Assert(minDuration(time.Minute, time.Second), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Minute), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Second), Equals, time.Second)
}

var _ = Suite(&testRegionUnhealthySuite{})

type testRegionUnhealthySuite struct{}

func (s *testRegionUnhealthySuite) TestIsRegionUnhealthy(c *C) {
	peers := make([]*metapb.Peer, 0, 3)
	for i := uint64(0); i < 2; i++ {
		p := &metapb.Peer{
			Id:      i,
			StoreId: i,
		}
		peers = append(peers, p)
	}
	peers = append(peers, &metapb.Peer{
		Id:      2,
		StoreId: 2,
	})

	r1 := core.NewRegionInfo(&metapb.Region{Peers: peers[:2]}, peers[0], core.WithLearners([]*metapb.Peer{peers[1]}))
	r2 := core.NewRegionInfo(&metapb.Region{Peers: peers[:2]}, peers[0], core.WithPendingPeers([]*metapb.Peer{peers[1]}))
	r4 := core.NewRegionInfo(&metapb.Region{Peers: peers[:2]}, peers[0])
	c.Assert(isRegionUnhealthy(r1), IsTrue)
	c.Assert(isRegionUnhealthy(r2), IsFalse)
	c.Assert(isRegionUnhealthy(r4), IsFalse)
}
