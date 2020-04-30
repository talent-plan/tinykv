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

package core

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	. "github.com/pingcap/check"
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct{}

func (s *testRegionSuite) TestRegionInfo(c *C) {
	n := uint64(3)

	peers := make([]*metapb.Peer, 0, n)
	for i := uint64(0); i < n; i++ {
		p := &metapb.Peer{
			Id:      i,
			StoreId: i,
		}
		peers = append(peers, p)
	}
	region := &metapb.Region{
		Peers: peers,
	}
	pendingPeer := peers[1]

	info := NewRegionInfo(
		region,
		peers[0],
		WithPendingPeers([]*metapb.Peer{pendingPeer}))

	r := info.Clone()
	c.Assert(r, DeepEquals, info)

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetPeer(i), Equals, r.meta.Peers[i])
	}
	c.Assert(r.GetPeer(n), IsNil)
	c.Assert(r.GetPendingPeer(n), IsNil)
	c.Assert(r.GetPendingPeer(pendingPeer.GetId()), DeepEquals, pendingPeer)

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetStorePeer(i).GetStoreId(), Equals, i)
	}
	c.Assert(r.GetStorePeer(n), IsNil)

	removePeer := &metapb.Peer{
		Id:      n,
		StoreId: n,
	}
	r = r.Clone(SetPeers(append(r.meta.Peers, removePeer)))
	c.Assert(DiffRegionPeersInfo(info, r), Matches, "Add peer.*")
	c.Assert(DiffRegionPeersInfo(r, info), Matches, "Remove peer.*")
	c.Assert(r.GetStorePeer(n), DeepEquals, removePeer)
	r = r.Clone(WithRemoveStorePeer(n))
	c.Assert(DiffRegionPeersInfo(r, info), Equals, "")
	c.Assert(r.GetStorePeer(n), IsNil)
	r = r.Clone(WithStartKey([]byte{0}))
	c.Assert(DiffRegionKeyInfo(r, info), Matches, "StartKey Changed.*")
	r = r.Clone(WithEndKey([]byte{1}))
	c.Assert(DiffRegionKeyInfo(r, info), Matches, ".*EndKey Changed.*")

	stores := r.GetStoreIds()
	c.Assert(stores, HasLen, int(n))
	for i := uint64(0); i < n; i++ {
		_, ok := stores[i]
		c.Assert(ok, IsTrue)
	}

	followers := r.GetFollowers()
	c.Assert(followers, HasLen, int(n-1))
	for i := uint64(1); i < n; i++ {
		c.Assert(followers[peers[i].GetStoreId()], DeepEquals, peers[i])
	}
}

func (s *testRegionSuite) TestRegionItem(c *C) {
	item := newRegionItem([]byte("b"), []byte{})

	c.Assert(item.Less(newRegionItem([]byte("a"), []byte{})), IsFalse)
	c.Assert(item.Less(newRegionItem([]byte("b"), []byte{})), IsFalse)
	c.Assert(item.Less(newRegionItem([]byte("c"), []byte{})), IsTrue)

	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)

	item = newRegionItem([]byte("b"), []byte("d"))
	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)
	c.Assert(item.Contains([]byte("d")), IsFalse)
}

func (s *testRegionSuite) TestRegionTree(c *C) {
	tree := newRegionTree()

	c.Assert(tree.search([]byte("a")), IsNil)

	regionA := NewTestRegionInfo([]byte("a"), []byte("b"))
	regionB := NewTestRegionInfo([]byte("b"), []byte("c"))
	regionC := NewTestRegionInfo([]byte("c"), []byte("d"))
	regionD := NewTestRegionInfo([]byte("d"), []byte{})

	tree.update(regionA)
	tree.update(regionC)
	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), IsNil)
	c.Assert(tree.search([]byte("c")), Equals, regionC)
	c.Assert(tree.search([]byte("d")), IsNil)

	// search previous region
	c.Assert(tree.searchPrev([]byte("a")), IsNil)
	c.Assert(tree.searchPrev([]byte("b")), IsNil)
	c.Assert(tree.searchPrev([]byte("c")), IsNil)

	tree.update(regionB)
	// search previous region
	c.Assert(tree.searchPrev([]byte("c")), Equals, regionB)
	c.Assert(tree.searchPrev([]byte("b")), Equals, regionA)

	tree.remove(regionC)
	tree.update(regionD)
	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), Equals, regionB)
	c.Assert(tree.search([]byte("c")), IsNil)
	c.Assert(tree.search([]byte("d")), Equals, regionD)

	// check get adjacent regions
	prev, next := tree.getAdjacentRegions(regionA)
	c.Assert(prev, IsNil)
	c.Assert(next.region, Equals, regionB)
	prev, next = tree.getAdjacentRegions(regionB)
	c.Assert(prev.region, Equals, regionA)
	c.Assert(next.region, Equals, regionD)
	prev, next = tree.getAdjacentRegions(regionC)
	c.Assert(prev.region, Equals, regionB)
	c.Assert(next.region, Equals, regionD)
	prev, next = tree.getAdjacentRegions(regionD)
	c.Assert(prev.region, Equals, regionB)
	c.Assert(next, IsNil)

	// region with the same range and different region id will not be delete.
	region0 := newRegionItem([]byte{}, []byte("a")).region
	tree.update(region0)
	c.Assert(tree.search([]byte{}), Equals, region0)
	anotherRegion0 := newRegionItem([]byte{}, []byte("a")).region
	anotherRegion0.meta.Id = 123
	tree.remove(anotherRegion0)
	c.Assert(tree.search([]byte{}), Equals, region0)

	// overlaps with 0, A, B, C.
	region0D := newRegionItem([]byte(""), []byte("d")).region
	tree.update(region0D)
	c.Assert(tree.search([]byte{}), Equals, region0D)
	c.Assert(tree.search([]byte("a")), Equals, region0D)
	c.Assert(tree.search([]byte("b")), Equals, region0D)
	c.Assert(tree.search([]byte("c")), Equals, region0D)
	c.Assert(tree.search([]byte("d")), Equals, regionD)

	// overlaps with D.
	regionE := newRegionItem([]byte("e"), []byte{}).region
	tree.update(regionE)
	c.Assert(tree.search([]byte{}), Equals, region0D)
	c.Assert(tree.search([]byte("a")), Equals, region0D)
	c.Assert(tree.search([]byte("b")), Equals, region0D)
	c.Assert(tree.search([]byte("c")), Equals, region0D)
	c.Assert(tree.search([]byte("d")), IsNil)
	c.Assert(tree.search([]byte("e")), Equals, regionE)
}

func updateRegions(c *C, tree *regionTree, regions []*RegionInfo) {
	for _, region := range regions {
		tree.update(region)
		c.Assert(tree.search(region.GetStartKey()), Equals, region)
		if len(region.GetEndKey()) > 0 {
			end := region.GetEndKey()[0]
			c.Assert(tree.search([]byte{end - 1}), Equals, region)
			c.Assert(tree.search([]byte{end + 1}), Not(Equals), region)
		}
	}
}

func (s *testRegionSuite) TestRegionTreeSplitAndMerge(c *C) {
	tree := newRegionTree()
	regions := []*RegionInfo{newRegionItem([]byte{}, []byte{}).region}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = SplitRegions(regions)
		updateRegions(c, tree, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = MergeRegions(regions)
		updateRegions(c, tree, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = MergeRegions(regions)
		} else {
			regions = SplitRegions(regions)
		}
		updateRegions(c, tree, regions)
	}
}

func (s *testRegionSuite) TestRandomRegion(c *C) {
	tree := newRegionTree()
	r := tree.RandomRegion([]byte(""), []byte(""))
	c.Assert(r, IsNil)

	regionA := NewTestRegionInfo([]byte(""), []byte("g"))
	tree.update(regionA)
	ra := tree.RandomRegion([]byte(""), []byte(""))
	c.Assert(ra, DeepEquals, regionA)

	regionB := NewTestRegionInfo([]byte("g"), []byte("n"))
	regionC := NewTestRegionInfo([]byte("n"), []byte("t"))
	regionD := NewTestRegionInfo([]byte("t"), []byte(""))
	tree.update(regionB)
	tree.update(regionC)
	tree.update(regionD)

	rb := tree.RandomRegion([]byte("g"), []byte("n"))
	c.Assert(rb, DeepEquals, regionB)
	rc := tree.RandomRegion([]byte("n"), []byte("t"))
	c.Assert(rc, DeepEquals, regionC)
	rd := tree.RandomRegion([]byte("t"), []byte(""))
	c.Assert(rd, DeepEquals, regionD)

	re := tree.RandomRegion([]byte("a"), []byte("a"))
	c.Assert(re, DeepEquals, regionA)
	re = tree.RandomRegion([]byte("o"), []byte("s"))
	c.Assert(re, DeepEquals, regionC)
	re = tree.RandomRegion([]byte(""), []byte("a"))
	c.Assert(re, DeepEquals, regionA)
	re = tree.RandomRegion([]byte("z"), []byte(""))
	c.Assert(re, DeepEquals, regionD)

	checkRandomRegion(c, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []byte(""), []byte(""))
	checkRandomRegion(c, tree, []*RegionInfo{regionA, regionB}, []byte(""), []byte("n"))
	checkRandomRegion(c, tree, []*RegionInfo{regionC, regionD}, []byte("n"), []byte(""))
	checkRandomRegion(c, tree, []*RegionInfo{regionB, regionC}, []byte("h"), []byte("s"))
	checkRandomRegion(c, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []byte("a"), []byte("z"))
}

func (s *testRegionSuite) TestRandomRegionDiscontinuous(c *C) {
	tree := newRegionTree()
	r := tree.RandomRegion([]byte("c"), []byte("f"))
	c.Assert(r, IsNil)

	// test for single region
	regionA := NewTestRegionInfo([]byte("c"), []byte("f"))
	tree.update(regionA)
	ra := tree.RandomRegion([]byte("c"), []byte("e"))
	c.Assert(ra, DeepEquals, regionA)
	ra = tree.RandomRegion([]byte("c"), []byte("f"))
	c.Assert(ra, DeepEquals, regionA)
	ra = tree.RandomRegion([]byte("c"), []byte("g"))
	c.Assert(ra, DeepEquals, regionA)
	ra = tree.RandomRegion([]byte("a"), []byte("e"))
	c.Assert(ra, DeepEquals, regionA)
	ra = tree.RandomRegion([]byte("a"), []byte("f"))
	c.Assert(ra, DeepEquals, regionA)
	ra = tree.RandomRegion([]byte("a"), []byte("g"))
	c.Assert(ra, DeepEquals, regionA)

	regionB := NewTestRegionInfo([]byte("n"), []byte("x"))
	tree.update(regionB)
	rb := tree.RandomRegion([]byte("g"), []byte("x"))
	c.Assert(rb, DeepEquals, regionB)
	rb = tree.RandomRegion([]byte("g"), []byte("y"))
	c.Assert(rb, DeepEquals, regionB)
	rb = tree.RandomRegion([]byte("n"), []byte("y"))
	c.Assert(rb, DeepEquals, regionB)
	rb = tree.RandomRegion([]byte("o"), []byte("y"))
	c.Assert(rb, DeepEquals, regionB)

	regionC := NewTestRegionInfo([]byte("z"), []byte(""))
	tree.update(regionC)
	rc := tree.RandomRegion([]byte("y"), []byte(""))
	c.Assert(rc, DeepEquals, regionC)
	regionD := NewTestRegionInfo([]byte(""), []byte("a"))
	tree.update(regionD)
	rd := tree.RandomRegion([]byte(""), []byte("b"))
	c.Assert(rd, DeepEquals, regionD)

	checkRandomRegion(c, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []byte(""), []byte(""))
}

func checkRandomRegion(c *C, tree *regionTree, regions []*RegionInfo, startKey, endKey []byte) {
	keys := make(map[string]struct{})
	for i := 0; i < 10000 && len(keys) < len(regions); i++ {
		re := tree.RandomRegion(startKey, endKey)
		c.Assert(re, NotNil)
		k := string(re.GetStartKey())
		if _, ok := keys[k]; !ok {
			keys[k] = struct{}{}
		}
	}
	for _, region := range regions {
		_, ok := keys[string(region.GetStartKey())]
		c.Assert(ok, IsTrue)
	}
	c.Assert(keys, HasLen, len(regions))
}

func newRegionItem(start, end []byte) *regionItem {
	return &regionItem{region: NewTestRegionInfo(start, end)}
}

func BenchmarkRegionTreeUpdate(b *testing.B) {
	tree := newRegionTree()
	for i := 0; i < b.N; i++ {
		item := &RegionInfo{meta: &metapb.Region{StartKey: []byte(fmt.Sprintf("%20d", i)), EndKey: []byte(fmt.Sprintf("%20d", i+1))}}
		tree.update(item)
	}
}

const MaxKey = 10000000

func BenchmarkRegionTreeUpdateUnordered(b *testing.B) {
	tree := newRegionTree()
	var items []*RegionInfo
	for i := 0; i < MaxKey; i++ {
		var startKey, endKey int
		key1 := rand.Intn(MaxKey)
		key2 := rand.Intn(MaxKey)
		if key1 < key2 {
			startKey = key1
			endKey = key2
		} else {
			startKey = key2
			endKey = key1
		}
		items = append(items, &RegionInfo{meta: &metapb.Region{StartKey: []byte(fmt.Sprintf("%20d", startKey)), EndKey: []byte(fmt.Sprintf("%20d", endKey))}})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.update(items[i])
	}
}
