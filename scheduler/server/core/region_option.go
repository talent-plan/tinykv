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

package core

import (
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

// RegionOption is used to select region.
type RegionOption func(region *RegionInfo) bool

// HealthRegion checks if the region is healthy.
func HealthRegion() RegionOption {
	return func(region *RegionInfo) bool {
		return len(region.pendingPeers) == 0 && len(region.learners) == 0
	}
}

// HealthRegionAllowPending checks if the region is healthy with allowing the pending peer.
func HealthRegionAllowPending() RegionOption {
	return func(region *RegionInfo) bool {
		return len(region.learners) == 0
	}
}

// RegionCreateOption used to create region.
type RegionCreateOption func(region *RegionInfo)

// WithPendingPeers sets the pending peers for the region.
func WithPendingPeers(pengdingPeers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.pendingPeers = pengdingPeers
	}
}

// WithLeader sets the leader for the region.
func WithLeader(leader *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.leader = leader
	}
}

// WithLearners adds learner to the region
func WithLearners(learner []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.learners = append(region.learners, learner...)
	}
}

// WithStartKey sets the start key for the region.
func WithStartKey(key []byte) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.StartKey = key
	}
}

// WithEndKey sets the end key for the region.
func WithEndKey(key []byte) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.EndKey = key
	}
}

// WithIncVersion increases the version of the region.
func WithIncVersion() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.Version++
		}
	}
}

// WithIncConfVer increases the config version of the region.
func WithIncConfVer() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.ConfVer++
		}
	}
}

// WithRemoveStorePeer removes the specified peer for the region.
func WithRemoveStorePeer(storeID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		var peers []*metapb.Peer
		for _, peer := range region.meta.GetPeers() {
			if peer.GetStoreId() != storeID {
				peers = append(peers, peer)
			}
		}
		region.meta.Peers = peers
	}
}

// SetApproximateSize sets the approximate size for the region.
func SetApproximateSize(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateSize = v
	}
}

// SetPeers sets the peers for the region.
func SetPeers(peers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = peers
	}
}

// WithAddPeer adds a peer for the region.
func WithAddPeer(peer *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = append(region.meta.Peers, peer)
		region.voters = append(region.voters, peer)
	}
}
