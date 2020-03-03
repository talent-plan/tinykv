package util

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap/errors"
)

const RaftInvalidIndex uint64 = 0
const InvalidID uint64 = 0

/// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
// 1. Target peer already exists but has not established communication with leader yet
// 2. Target peer is added newly due to member change or region split, but it's not
//    created yet
// For both cases the region start key and end key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a pending region split to perform
// later.
func IsInitialMsg(msg *eraftpb.Message) bool {
	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
		// the peer has not been known to this leader, it may exist or not.
		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
}

/// Check if key in region range [`start_key`, `end_key`).
func CheckKeyInRegion(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

/// Check if key in region range (`start_key`, `end_key`).
func CheckKeyInRegionExclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(region.StartKey, key) < 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

/// Check if key in region range [`start_key`, `end_key`].
func CheckKeyInRegionInclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) <= 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

/// check whether epoch is staler than check_epoch.
func IsEpochStale(epoch *metapb.RegionEpoch, checkEpoch *metapb.RegionEpoch) bool {
	return epoch.Version < checkEpoch.Version || epoch.ConfVer < checkEpoch.ConfVer
}

func IsVoteMessage(msg *eraftpb.Message) bool {
	tp := msg.GetMsgType()
	return tp == eraftpb.MessageType_MsgRequestVote
}

/// `is_first_vote_msg` checks `msg` is the first vote message or not. It's used for
/// when the message is received but there is no such region in `Store::region_peers` and the
/// region overlaps with others. In this case we should put `msg` into `pending_votes` instead of
/// create the peer.
func IsFirstVoteMessage(msg *eraftpb.Message) bool {
	return IsVoteMessage(msg) && msg.Term == meta.RaftInitLogTerm+1
}

func CheckRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	checkVer, checkConfVer := false, false
	if req.AdminRequest == nil {
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_BatchSplit, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}

	if !checkVer && !checkConfVer {
		return nil
	}

	if req.Header == nil {
		return fmt.Errorf("missing header!")
	}

	if req.Header.RegionEpoch == nil {
		return fmt.Errorf("missing epoch!")
	}

	fromEpoch := req.Header.RegionEpoch
	currentEpoch := region.RegionEpoch

	// We must check epochs strictly to avoid key not in region error.
	//
	// A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
	// tells TiDB with a epoch not match error contains the latest target Region
	// info, TiDB updates its region cache and sends requests to TiKV B,
	// and TiKV B has not applied commit merge yet, since the region epoch in
	// request is higher than TiKV B, the request must be denied due to epoch
	// not match, so it does not read on a stale snapshot, thus avoid the
	// KeyNotInRegion error.
	if (checkConfVer && fromEpoch.ConfVer != currentEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version != currentEpoch.Version) {
		log.Debugf("epoch not match, region id %v, from epoch %v, current epoch %v",
			region.Id, fromEpoch, currentEpoch)

		regions := []*metapb.Region{}
		if includeRegion {
			regions = []*metapb.Region{region}
		}
		return &ErrEpochNotMatch{Message: fmt.Sprintf("current epoch of region %v is %v, but you sent %v",
			region.Id, currentEpoch, fromEpoch), Regions: regions}
	}

	return nil
}

func FindPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			return peer
		}
	}
	return nil
}

func RemovePeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for i, peer := range region.Peers {
		if peer.StoreId == storeID {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return peer
		}
	}
	return nil
}

func ConfStateFromRegion(region *metapb.Region) (confState eraftpb.ConfState) {
	for _, p := range region.Peers {
		confState.Nodes = append(confState.Nodes, p.GetId())
	}
	return
}

func CheckStoreID(req *raft_cmdpb.RaftCmdRequest, storeID uint64) error {
	peer := req.Header.Peer
	if peer.StoreId == storeID {
		return nil
	}
	return errors.Errorf("store not match %d %d", peer.StoreId, storeID)
}

func CheckTerm(req *raft_cmdpb.RaftCmdRequest, term uint64) error {
	header := req.Header
	if header.Term == 0 || term <= header.Term+1 {
		return nil
	}
	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	return &ErrStaleCommand{}
}

func CheckPeerID(req *raft_cmdpb.RaftCmdRequest, peerID uint64) error {
	peer := req.Header.Peer
	if peer.Id == peerID {
		return nil
	}
	return errors.Errorf("mismatch peer id %d != %d", peer.Id, peerID)
}

func CloneMsg(origin, cloned proto.Message) error {
	data, err := proto.Marshal(origin)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, cloned)
}

func SafeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func PeerEqual(l, r *metapb.Peer) bool {
	return l.Id == r.Id && l.StoreId == r.StoreId
}

func RegionEqual(l, r *metapb.Region) bool {
	if l == nil || r == nil {
		return false
	}
	return l.Id == r.Id && l.RegionEpoch.Version == r.RegionEpoch.Version && l.RegionEpoch.ConfVer == r.RegionEpoch.ConfVer
}
