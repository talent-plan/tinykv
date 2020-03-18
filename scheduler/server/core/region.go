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
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
)

// RegionInfo records detail region info.
// Read-Only once created.
type RegionInfo struct {
	meta            *metapb.Region
	learners        []*metapb.Peer
	voters          []*metapb.Peer
	leader          *metapb.Peer
	pendingPeers    []*metapb.Peer
	approximateSize int64
}

// NewRegionInfo creates RegionInfo with region's meta and leader peer.
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer, opts ...RegionCreateOption) *RegionInfo {
	regionInfo := &RegionInfo{
		meta:   region,
		leader: leader,
	}

	for _, opt := range opts {
		opt(regionInfo)
	}
	classifyVoterAndLearner(regionInfo)
	return regionInfo
}

// classifyVoterAndLearner sorts out voter and learner from peers into different slice.
func classifyVoterAndLearner(region *RegionInfo) {
	voters := make([]*metapb.Peer, 0, len(region.meta.Peers))
	for _, p := range region.meta.Peers {
		voters = append(voters, p)
	}
	region.voters = voters
}

// EmptyRegionApproximateSize is the region approximate size of an empty region
// (heartbeat size <= 1MB).
const EmptyRegionApproximateSize = 1

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *schedulerpb.RegionHeartbeatRequest) *RegionInfo {
	// Convert unit to MB.
	// If region is empty or less than 1MB, use 1MB instead.
	regionSize := heartbeat.GetApproximateSize() / (1 << 20)
	if regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		meta:            heartbeat.GetRegion(),
		leader:          heartbeat.GetLeader(),
		pendingPeers:    heartbeat.GetPendingPeers(),
		approximateSize: int64(regionSize),
	}

	classifyVoterAndLearner(region)
	return region
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone(opts ...RegionCreateOption) *RegionInfo {
	pendingPeers := make([]*metapb.Peer, 0, len(r.pendingPeers))
	for _, peer := range r.pendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}

	region := &RegionInfo{
		meta:            proto.Clone(r.meta).(*metapb.Region),
		leader:          proto.Clone(r.leader).(*metapb.Peer),
		pendingPeers:    pendingPeers,
		approximateSize: r.approximateSize,
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// GetLearners returns the learners.
func (r *RegionInfo) GetLearners() []*metapb.Peer {
	return r.learners
}

// GetVoters returns the voters.
func (r *RegionInfo) GetVoters() []*metapb.Peer {
	return r.voters
}

// GetPeer returns the peer with specified peer id.
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *RegionInfo) GetDownLearner(peerID uint64) *metapb.Peer {
	return nil
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingVoter returns the pending voter with specified peer id.
func (r *RegionInfo) GetPendingVoter(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *RegionInfo) GetPendingLearner(peerID uint64) *metapb.Peer {
	return nil
}

// GetStorePeer returns the peer in specified store.
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreVoter returns the voter in specified store.
func (r *RegionInfo) GetStoreVoter(storeID uint64) *metapb.Peer {
	for _, peer := range r.voters {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreLearner returns the learner peer in specified store.
func (r *RegionInfo) GetStoreLearner(storeID uint64) *metapb.Peer {
	for _, peer := range r.learners {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreIds returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.meta.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetVoters()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetVoters() {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// GetDiffFollowers returns the followers which is not located in the same
// store as any other followers of the another specified region.
func (r *RegionInfo) GetDiffFollowers(other *RegionInfo) []*metapb.Peer {
	res := make([]*metapb.Peer, 0, len(r.meta.Peers))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.GetStoreId() == o.GetStoreId() {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// GetID returns the ID of the region.
func (r *RegionInfo) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns the meta information of the region.
func (r *RegionInfo) GetMeta() *metapb.Region {
	return r.meta
}

// GetApproximateSize returns the approximate size of the region.
func (r *RegionInfo) GetApproximateSize() int64 {
	return r.approximateSize
}

// GetPendingPeers returns the pending peers of the region.
func (r *RegionInfo) GetPendingPeers() []*metapb.Peer {
	return r.pendingPeers
}

// GetLeader returns the leader of the region.
func (r *RegionInfo) GetLeader() *metapb.Peer {
	return r.leader
}

// GetStartKey returns the start key of the region.
func (r *RegionInfo) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetEndKey returns the end key of the region.
func (r *RegionInfo) GetEndKey() []byte {
	return r.meta.EndKey
}

// GetPeers returns the peers of the region.
func (r *RegionInfo) GetPeers() []*metapb.Peer {
	return r.meta.GetPeers()
}

// GetRegionEpoch returns the region epoch of the region.
func (r *RegionInfo) GetRegionEpoch() *metapb.RegionEpoch {
	return r.meta.RegionEpoch
}

// regionMap wraps a map[uint64]*core.RegionInfo and supports randomly pick a region.
type regionMap struct {
	m         map[uint64]*RegionInfo
	totalSize int64
	totalKeys int64
}

func newRegionMap() *regionMap {
	return &regionMap{
		m: make(map[uint64]*RegionInfo),
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetID()]; ok {
		rm.totalSize -= old.approximateSize
	}
	rm.m[region.GetID()] = region
	rm.totalSize += region.approximateSize
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		delete(rm.m, id)
		rm.totalSize -= old.approximateSize
	}
}

func (rm *regionMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// regionSubTree is used to manager different types of regions.
type regionSubTree struct {
	*regionTree
	totalSize int64
}

func newRegionSubTree() *regionSubTree {
	return &regionSubTree{
		regionTree: newRegionTree(),
		totalSize:  0,
	}
}

func (rst *regionSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *regionSubTree) scanRanges() []*RegionInfo {
	if rst.length() == 0 {
		return nil
	}
	var res []*RegionInfo
	rst.scanRange([]byte(""), func(region *RegionInfo) bool {
		res = append(res, region)
		return true
	})
	return res
}

func (rst *regionSubTree) update(region *RegionInfo) {
	if r := rst.find(region); r != nil {
		rst.totalSize += region.approximateSize - r.region.approximateSize
		r.region = region
		return
	}
	rst.totalSize += region.approximateSize
	rst.regionTree.update(region)
}

func (rst *regionSubTree) remove(region *RegionInfo) {
	if rst.length() == 0 {
		return
	}
	rst.regionTree.remove(region)
}

func (rst *regionSubTree) length() int {
	if rst == nil {
		return 0
	}
	return rst.regionTree.length()
}

func (rst *regionSubTree) RandomRegion(startKey, endKey []byte) *RegionInfo {
	if rst.length() == 0 {
		return nil
	}
	return rst.regionTree.RandomRegion(startKey, endKey)
}

// RegionsInfo for export
type RegionsInfo struct {
	tree         *regionTree
	regions      *regionMap                // regionID -> regionInfo
	leaders      map[uint64]*regionSubTree // storeID -> regionSubTree
	followers    map[uint64]*regionSubTree // storeID -> regionSubTree
	learners     map[uint64]*regionSubTree // storeID -> regionSubTree
	pendingPeers map[uint64]*regionSubTree // storeID -> regionSubTree
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionSubTree),
		followers:    make(map[uint64]*regionSubTree),
		learners:     make(map[uint64]*regionSubTree),
		pendingPeers: make(map[uint64]*regionSubTree),
	}
}

// GetRegion returns the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region
}

// SetRegion sets the RegionInfo with regionID
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*RegionInfo {
	if origin := r.regions.Get(region.GetID()); origin != nil {
		r.RemoveRegion(origin)
	}
	return r.AddRegion(region)
}

// Length returns the RegionsInfo length
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength returns the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*RegionInfo {
	return r.tree.getOverlaps(region)
}

// AddRegion adds RegionInfo to regionTree and regionMap, also update leaders and followers by region peers
func (r *RegionsInfo) AddRegion(region *RegionInfo) []*RegionInfo {
	// Add to tree and regions.
	overlaps := r.tree.update(region)
	for _, item := range overlaps {
		r.RemoveRegion(r.GetRegion(item.GetID()))
	}

	r.regions.Put(region)

	// Add to leaders and followers.
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionSubTree()
				r.leaders[storeID] = store
			}
			store.update(region)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionSubTree()
				r.followers[storeID] = store
			}
			store.update(region)
		}
	}

	// Add to learners.
	for _, peer := range region.GetLearners() {
		storeID := peer.GetStoreId()
		store, ok := r.learners[storeID]
		if !ok {
			store = newRegionSubTree()
			r.learners[storeID] = store
		}
		store.update(region)
	}

	for _, peer := range region.pendingPeers {
		storeID := peer.GetStoreId()
		store, ok := r.pendingPeers[storeID]
		if !ok {
			store = newRegionSubTree()
			r.pendingPeers[storeID] = store
		}
		store.update(region)
	}

	return overlaps
}

// RemoveRegion removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region)
	r.regions.Delete(region.GetID())
	// Remove from leaders and followers.
	for _, peer := range region.meta.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].remove(region)
		r.followers[storeID].remove(region)
		r.learners[storeID].remove(region)
		r.pendingPeers[storeID].remove(region)
	}
}

// SearchRegion searches RegionInfo from regionTree
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// SearchPrevRegion searches previous RegionInfo from regionTree
func (r *RegionsInfo) SearchPrevRegion(regionKey []byte) *RegionInfo {
	region := r.tree.searchPrev(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// GetRegions gets all RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region)
	}
	return regions
}

// GetStoreRegions gets all RegionInfo with a given storeID
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.GetStoreLeaderCount(storeID)+r.GetStoreFollowerCount(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		for _, region := range leaders.scanRanges() {
			regions = append(regions, region)
		}
	}
	if followers, ok := r.followers[storeID]; ok {
		for _, region := range followers.scanRanges() {
			regions = append(regions, region)
		}
	}
	return regions
}

// GetStoreLeaderRegionSize get total size of store's leader regions
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize get total size of store's follower regions
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	return r.followers[storeID].TotalSize()
}

// GetStoreLearnerRegionSize get total size of store's learner regions
func (r *RegionsInfo) GetStoreLearnerRegionSize(storeID uint64) int64 {
	return r.learners[storeID].TotalSize()
}

// GetStoreRegionSize get total size of store's regions
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	return r.GetStoreLeaderRegionSize(storeID) + r.GetStoreFollowerRegionSize(storeID) + r.GetStoreLearnerRegionSize(storeID)
}

// GetMetaRegions gets a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.meta).(*metapb.Region))
	}
	return regions
}

// GetRegionCount gets the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount gets the total count of a store's leader and follower RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID) + r.GetStoreLearnerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].length()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].length()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].length()
}

// GetStoreLearnerCount get the total count of a store's learner RegionInfo
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	return r.learners[storeID].length()
}

// RandRegion get a region by random
func (r *RegionsInfo) RandRegion(opts ...RegionOption) *RegionInfo {
	return randRegion(r.tree, opts...)
}

// RandPendingRegion randomly gets a store's region with a pending peer.
func (r *RegionsInfo) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.pendingPeers[storeID], opts...)
}

// RandLeaderRegion randomly gets a store's leader region.
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.leaders[storeID], opts...)
}

// RandFollowerRegion randomly gets a store's follower region.
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.followers[storeID], opts...)
}

// GetPendingRegionsWithLock return pending regions subtree by storeID
func (r *RegionsInfo) GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.pendingPeers[storeID])
}

// GetLeadersWithLock return leaders subtree by storeID
func (r *RegionsInfo) GetLeadersWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.leaders[storeID])
}

// GetFollowersWithLock return leaders subtree by storeID
func (r *RegionsInfo) GetFollowersWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.followers[storeID])
}

// GetLeader return leader RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, region *RegionInfo) *RegionInfo {
	return r.leaders[storeID].find(region).region
}

// GetFollower return follower RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, region *RegionInfo) *RegionInfo {
	return r.followers[storeID].find(region).region
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(region *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, r.GetRegion(region.GetID()))
		return true
	})
	return res
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	if r.regions.Len() == 0 {
		return 0
	}
	return r.regions.TotalSize() / int64(r.regions.Len())
}

const randomRegionMaxRetry = 10

// RegionsContainer is a container to store regions.
type RegionsContainer interface {
	RandomRegion(startKey, endKey []byte) *RegionInfo
}

func randRegion(regions RegionsContainer, opts ...RegionOption) *RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion(nil, nil)
		if region == nil {
			return nil
		}
		isSelect := true
		for _, opt := range opts {
			if !opt(region) {
				isSelect = false
				break
			}
		}
		if isSelect {
			return region
		}
	}
	return nil
}

// DiffRegionPeersInfo return the difference of peers info  between two RegionInfo
func DiffRegionPeersInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	for _, a := range origin.meta.Peers {
		both := false
		for _, b := range other.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.meta.Peers {
		both := false
		for _, a := range origin.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffRegionKeyInfo return the difference of key info between two RegionInfo
func DiffRegionKeyInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	if !bytes.Equal(origin.meta.StartKey, other.meta.StartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.StartKey), HexRegionKey(other.meta.StartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexRegionKey(origin.meta.StartKey)))
	}
	if !bytes.Equal(origin.meta.EndKey, other.meta.EndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.EndKey), HexRegionKey(other.meta.EndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexRegionKey(origin.meta.EndKey)))
	}

	return strings.Join(ret, ", ")
}

// HexRegionKey converts region key to hex format. Used for formating region in
// logs.
func HexRegionKey(key []byte) []byte {
	return []byte(strings.ToUpper(hex.EncodeToString(key)))
}

// RegionToHexMeta converts a region meta's keys to hex format. Used for formating
// region in logs.
func RegionToHexMeta(meta *metapb.Region) HexRegionMeta {
	if meta == nil {
		return HexRegionMeta{}
	}
	meta = proto.Clone(meta).(*metapb.Region)
	meta.StartKey = HexRegionKey(meta.StartKey)
	meta.EndKey = HexRegionKey(meta.EndKey)
	return HexRegionMeta{meta}
}

// HexRegionMeta is a region meta in the hex format. Used for formating region in logs.
type HexRegionMeta struct {
	*metapb.Region
}

func (h HexRegionMeta) String() string {
	return strings.TrimSpace(proto.CompactTextString(h.Region))
}

// RegionsToHexMeta converts regions' meta keys to hex format. Used for formating
// region in logs.
func RegionsToHexMeta(regions []*metapb.Region) HexRegionsMeta {
	hexRegionMetas := make([]*metapb.Region, len(regions))
	for i, region := range regions {
		meta := proto.Clone(region).(*metapb.Region)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)

		hexRegionMetas[i] = meta
	}
	return HexRegionsMeta(hexRegionMetas)
}

// HexRegionsMeta is a slice of regions' meta in the hex format. Used for formating
// region in logs.
type HexRegionsMeta []*metapb.Region

func (h HexRegionsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(proto.CompactTextString(r))
	}
	return strings.TrimSpace(b.String())
}
