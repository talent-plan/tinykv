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
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
)

// RegionOption is used to select region.
type RegionOption func(region *RegionInfo) bool

// HealthRegion checks if the region is healthy.
func HealthRegion() RegionOption {
	return func(region *RegionInfo) bool {
		return len(region.downPeers) == 0 && len(region.pendingPeers) == 0 && len(region.learners) == 0
	}
}

// HealthRegionAllowPending checks if the region is healthy with allowing the pending peer.
func HealthRegionAllowPending() RegionOption {
	return func(region *RegionInfo) bool {
		return len(region.downPeers) == 0 && len(region.learners) == 0
	}
}

// RegionCreateOption used to create region.
type RegionCreateOption func(region *RegionInfo)

// WithDownPeers sets the down peers for the region.
func WithDownPeers(downPeers []*pdpb.PeerStats) RegionCreateOption {
	return func(region *RegionInfo) {
		region.downPeers = downPeers
	}
}

// WithPendingPeers sets the pending peers for the region.
func WithPendingPeers(pengdingPeers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.pendingPeers = pengdingPeers
	}
}

// WithLearners sets the learners for the region.
func WithLearners(learners []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.learners = learners
	}
}

// WithLeader sets the leader for the region.
func WithLeader(leader *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.leader = leader
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

// WithNewRegionID sets new id for the region.
func WithNewRegionID(id uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Id = id
	}
}

// WithNewPeerIds sets new ids for peers.
func WithNewPeerIds(peerIds ...uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if len(peerIds) != len(region.meta.GetPeers()) {
			return
		}
		for i, p := range region.meta.GetPeers() {
			p.Id = peerIds[i]
		}
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

// WithDecVersion decreases the version of the region.
func WithDecVersion() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.Version--
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

// WithDecConfVer decreases the config version of the region.
func WithDecConfVer() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.ConfVer--
		}
	}
}

// SetWrittenBytes sets the written bytes for the region.
func SetWrittenBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.writtenBytes = v
	}
}

// SetWrittenKeys sets the written keys for the region.
func SetWrittenKeys(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.writtenKeys = v
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

// SetReadBytes sets the read bytes for the region.
func SetReadBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.readBytes = v
	}
}

// SetReadKeys sets the read keys for the region.
func SetReadKeys(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.readKeys = v
	}
}

// SetApproximateSize sets the approximate size for the region.
func SetApproximateSize(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateSize = v
	}
}

// SetApproximateKeys sets the approximate keys for the region.
func SetApproximateKeys(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateKeys = v
	}
}

// SetReportInterval sets the report interval for the region.
func SetReportInterval(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.interval = &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: v}
	}
}

// SetRegionConfVer sets the config version for the reigon.
func SetRegionConfVer(confVer uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if region.meta.RegionEpoch == nil {
			region.meta.RegionEpoch = &metapb.RegionEpoch{ConfVer: confVer, Version: 1}
		} else {
			region.meta.RegionEpoch.ConfVer = confVer
		}
	}
}

// SetRegionVersion sets the version for the reigon.
func SetRegionVersion(version uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if region.meta.RegionEpoch == nil {
			region.meta.RegionEpoch = &metapb.RegionEpoch{ConfVer: 1, Version: version}
		} else {
			region.meta.RegionEpoch.Version = version
		}
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
		if peer.IsLearner {
			region.learners = append(region.learners, peer)
		} else {
			region.voters = append(region.voters, peer)
		}
	}
}

// WithPromoteLearner promotes the learner.
func WithPromoteLearner(peerID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		for _, p := range region.GetPeers() {
			if p.GetId() == peerID {
				p.IsLearner = false
			}
		}
	}
}
