package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
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
func isInitialMsg(msg *eraftpb.Message) bool {
	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
		msg.MsgType == eraftpb.MessageType_MsgRequestPreVote ||
		// the peer has not been known to this leader, it may exist or not.
		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
}

type LeaseState int

const (
	/// The lease is suspicious, may be invalid.
	LeaseState_Suspect LeaseState = 1 + iota
	/// The lease is valid.
	LeaseState_Valid
	/// The lease is expired.
	LeaseState_Expired
)

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the Raft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when raft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `send_ts + max_lease` in short.
type Lease struct {
	// If boundSuspect is not nil, then boundValid must be nil, if boundValid is not nil, then
	// boundSuspect must be nil
	boundSuspect *time.Time
	boundValid   *time.Time
	maxLease     time.Duration

	maxDrift   time.Duration
	lastUpdate time.Time
	remote     *RemoteLease

	// Todo: use monotonic_raw instead of time.Now() to fix time jump back issue.
}

func NewLease(maxLease time.Duration) *Lease {
	return &Lease{
		maxLease:   maxLease,
		maxDrift:   maxLease / 3,
		lastUpdate: time.Time{},
	}
}

/// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
/// And the expired timestamp for that leader lease is `commit_ts + lease`,
/// which is `send_ts + max_lease` in short.
func (l *Lease) nextExpiredTime(sendTs time.Time) time.Time {
	return sendTs.Add(l.maxLease)
}

/// Renew the lease to the bound.
func (l *Lease) Renew(sendTs time.Time) {
	bound := l.nextExpiredTime(sendTs)
	if l.boundSuspect != nil {
		// Longer than suspect ts
		if l.boundSuspect.Before(bound) {
			l.boundSuspect = nil
			l.boundValid = &bound
		}
	} else if l.boundValid != nil {
		// Longer than valid ts
		if l.boundValid.Before(bound) {
			l.boundValid = &bound
		}
	} else {
		// Or an empty lease
		l.boundValid = &bound
	}

	// Renew remote if it's valid.
	if l.boundValid != nil {
		if l.boundValid.Sub(l.lastUpdate) > l.maxDrift {
			l.lastUpdate = *l.boundValid
			if l.remote != nil {
				l.remote.Renew(*l.boundValid)
			}
		}
	}
}

/// Suspect the lease to the bound.
func (l *Lease) Suspect(sendTs time.Time) {
	l.ExpireRemoteLease()
	bound := l.nextExpiredTime(sendTs)
	l.boundValid = nil
	l.boundSuspect = &bound
}

/// Inspect the lease state for the ts or now.
func (l *Lease) Inspect(ts *time.Time) LeaseState {
	if l.boundSuspect != nil {
		return LeaseState_Suspect
	}
	if l.boundValid != nil {
		if ts == nil {
			t := time.Now()
			ts = &t
		}
		if ts.Before(*l.boundValid) {
			return LeaseState_Valid
		} else {
			return LeaseState_Expired
		}
	}
	return LeaseState_Expired
}

func (l *Lease) Expire() {
	l.ExpireRemoteLease()
	l.boundValid = nil
	l.boundSuspect = nil
}

func (l *Lease) ExpireRemoteLease() {
	// Expire remote lease if there is any.
	if l.remote != nil {
		l.remote.Expire()
		l.remote = nil
	}
}

/// Return a new `RemoteLease` if there is none.
func (l *Lease) MaybeNewRemoteLease(term uint64) *RemoteLease {
	if l.remote != nil {
		if l.remote.term == term {
			// At most one connected RemoteLease in the same term.
			return nil
		} else {
			// Term has changed. It is unreachable in the current implementation,
			// because we expire remote lease when leaders step down.
			panic("Must expire the old remote lease first!")
		}
	}
	expiredTime := uint64(0)
	if l.boundValid != nil {
		expiredTime = TimeToU64(*l.boundValid)
	}
	remote := &RemoteLease{
		expiredTime: &expiredTime,
		term:        term,
	}
	// Clone the remote.
	remoteClone := &RemoteLease{
		expiredTime: &expiredTime,
		term:        term,
	}
	l.remote = remote
	return remoteClone
}

/// A remote lease, it can only be derived by `Lease`. It will be sent
/// to the local read thread, so name it remote. If Lease expires, the remote must
/// expire too.
type RemoteLease struct {
	expiredTime *uint64
	term        uint64
}

func (r *RemoteLease) Inspect(ts *time.Time) LeaseState {
	expiredTime := atomic.LoadUint64(r.expiredTime)
	if ts == nil {
		t := time.Now()
		ts = &t
	}
	if ts.Before(U64ToTime(expiredTime)) {
		return LeaseState_Valid
	} else {
		return LeaseState_Expired
	}
}

func (r *RemoteLease) Renew(bound time.Time) {
	atomic.StoreUint64(r.expiredTime, TimeToU64(bound))
}

func (r *RemoteLease) Expire() {
	atomic.StoreUint64(r.expiredTime, 0)
}

func (r *RemoteLease) Term() uint64 {
	return r.term
}

const (
	NSEC_PER_MSEC uint64 = 1000000
	SEC_SHIFT     uint64 = 10
	MSEC_MASK     uint64 = (1 << SEC_SHIFT) - 1
)

func TimeToU64(t time.Time) uint64 {
	sec := uint64(t.Unix())
	msec := uint64(t.Nanosecond()) / NSEC_PER_MSEC
	sec <<= SEC_SHIFT
	return sec | msec
}

func U64ToTime(u uint64) time.Time {
	sec := u >> SEC_SHIFT
	nsec := (u & MSEC_MASK) * NSEC_PER_MSEC
	return time.Unix(int64(sec), int64(nsec))
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

func CheckRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	checkVer, checkConfVer := false, false
	if req.AdminRequest == nil {
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin,
			raft_cmdpb.AdminCmdType_ComputeHash, raft_cmdpb.AdminCmdType_VerifyHash:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit,
			raft_cmdpb.AdminCmdType_PrepareMerge, raft_cmdpb.AdminCmdType_CommitMerge,
			raft_cmdpb.AdminCmdType_RollbackMerge, raft_cmdpb.AdminCmdType_TransferLeader:
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

func findPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			return peer
		}
	}
	return nil
}

func removePeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for i, peer := range region.Peers {
		if peer.StoreId == storeID {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return peer
		}
	}
	return nil
}

func isVoteMessage(msg *eraftpb.Message) bool {
	tp := msg.GetMsgType()
	return tp == eraftpb.MessageType_MsgRequestVote || tp == eraftpb.MessageType_MsgRequestPreVote
}

/// `is_first_vote_msg` checks `msg` is the first vote (or prevote) message or not. It's used for
/// when the message is received but there is no such region in `Store::region_peers` and the
/// region overlaps with others. In this case we should put `msg` into `pending_votes` instead of
/// create the peer.
func isFirstVoteMessage(msg *eraftpb.Message) bool {
	return isVoteMessage(msg) && msg.Term == RaftInitLogTerm+1
}

func regionIDToBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)
	return b
}

func regionIDFromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func checkRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	var checkVer, checkConfVer bool
	if req.AdminRequest == nil {
		// for get/set/delete, we don't care conf_version.
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin,
			raft_cmdpb.AdminCmdType_ComputeHash, raft_cmdpb.AdminCmdType_VerifyHash:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit,
			raft_cmdpb.AdminCmdType_PrepareMerge, raft_cmdpb.AdminCmdType_CommitMerge,
			raft_cmdpb.AdminCmdType_RollbackMerge, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}
	if !checkVer && !checkConfVer {
		return nil
	}
	if req.Header.RegionEpoch == nil {
		return errors.Errorf("missing epoch")
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
		err := &ErrEpochNotMatch{}
		if includeRegion {
			err.Regions = []*metapb.Region{region}
		}
		err.Message = fmt.Sprintf("current epoch of region %d is %s, but you sent %s",
			region.Id, currentEpoch, fromEpoch)
		return err
	}
	return nil
}

func checkStoreID(req *raft_cmdpb.RaftCmdRequest, storeID uint64) error {
	peer := req.Header.Peer
	if peer.StoreId == storeID {
		return nil
	}
	return errors.Errorf("store not match %d %d", peer.StoreId, storeID)
}

func checkTerm(req *raft_cmdpb.RaftCmdRequest, term uint64) error {
	header := req.Header
	if header.Term == 0 || term <= header.Term+1 {
		return nil
	}
	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	return &ErrStaleCommand{}
}

func checkPeerID(req *raft_cmdpb.RaftCmdRequest, peerID uint64) error {
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
