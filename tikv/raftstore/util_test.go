package raftstore

import (
	"testing"
	"time"

	"github.com/ngaut/unistore/pkg/eraftpb"
	"github.com/ngaut/unistore/pkg/metapb"
	"github.com/ngaut/unistore/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLease(t *testing.T) {
	sleepTest := func(duration time.Duration, lease *Lease, state LeaseState) {
		time.Sleep(duration)
		end := time.Now()
		assert.Equal(t, lease.Inspect(&end), state)
		assert.Equal(t, lease.Inspect(nil), state)
	}

	duration := 1500 * time.Millisecond

	// Empty lease.
	lease := NewLease(duration)
	remote := lease.MaybeNewRemoteLease(1)
	require.NotNil(t, remote)
	inspectTest := func(lease *Lease, ts *time.Time, state LeaseState) {
		assert.Equal(t, lease.Inspect(ts), state)
		if state == LeaseState_Expired || state == LeaseState_Suspect {
			assert.Equal(t, remote.Inspect(ts), LeaseState_Expired)
		}
	}

	now := time.Now()
	inspectTest(lease, &now, LeaseState_Expired)

	now = time.Now()
	nextExpiredTime := lease.nextExpiredTime(now)
	assert.Equal(t, now.Add(duration), nextExpiredTime)

	// Transit to the Valid state.
	now = time.Now()
	lease.Renew(now)
	inspectTest(lease, &now, LeaseState_Valid)
	inspectTest(lease, nil, LeaseState_Valid)

	// After lease expired time.
	sleepTest(duration, lease, LeaseState_Expired)
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Expired)
	inspectTest(lease, nil, LeaseState_Expired)

	// Transit to the Suspect state.
	now = time.Now()
	lease.Suspect(now)
	inspectTest(lease, &now, LeaseState_Suspect)
	inspectTest(lease, nil, LeaseState_Suspect)

	// After lease expired time, still suspect.
	sleepTest(duration, lease, LeaseState_Suspect)
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Suspect)

	// Clear lease.
	lease.Expire()
	now = time.Now()
	inspectTest(lease, &now, LeaseState_Expired)
	inspectTest(lease, nil, LeaseState_Expired)

	// An expired remote lease can never renew.
	now = time.Now()
	lease.Renew(now.Add(1 * time.Minute))
	assert.Equal(t, remote.Inspect(&now), LeaseState_Expired)

	// A new remote lease.
	m1 := lease.MaybeNewRemoteLease(1)
	require.NotNil(t, m1)
	now = time.Now()
	assert.Equal(t, m1.Inspect(&now), LeaseState_Valid)
}

func TestTimeU64(t *testing.T) {
	type TimeU64 struct {
		T time.Time
		U uint64
	}
	testsTimeToU64 := []TimeU64{
		TimeU64{T: time.Unix(0, 0), U: 0},
		TimeU64{T: time.Unix(0, 1), U: 0},      // 1ns will be rounded down to 0ms
		TimeU64{T: time.Unix(0, 999999), U: 0}, // 999999ns will be rounded down to 0ms
		TimeU64{T: time.Unix(1, 0), U: 1 << SEC_SHIFT},
		TimeU64{T: time.Unix(1, int64(NSEC_PER_MSEC)), U: (1 << SEC_SHIFT) + 1},
	}

	for _, test := range testsTimeToU64 {
		assert.Equal(t, TimeToU64(test.T), test.U)
	}

	testsU64ToTime := []TimeU64{
		TimeU64{T: time.Unix(0, 0), U: 0},
		TimeU64{T: time.Unix(0, int64(NSEC_PER_MSEC)), U: 1},
		TimeU64{T: time.Unix(1, 0), U: 1 << SEC_SHIFT},
		TimeU64{T: time.Unix(1, int64(NSEC_PER_MSEC)), U: (1 << SEC_SHIFT) + 1},
	}
	for _, test := range testsU64ToTime {
		assert.Equal(t, U64ToTime(test.U), test.T)
	}
}

func TestCheckKeyInRegion(t *testing.T) {
	type Case struct {
		Key        []byte
		StartKey   []byte
		EndKey     []byte
		IsInRegion bool
		Inclusive  bool
		Exclusive  bool
	}
	test_cases := []Case{
		{Key: []byte{}, StartKey: []byte{}, EndKey: []byte{}, IsInRegion: true, Inclusive: true, Exclusive: false},
		{Key: []byte{}, StartKey: []byte{}, EndKey: []byte{6}, IsInRegion: true, Inclusive: true, Exclusive: false},
		{Key: []byte{}, StartKey: []byte{3}, EndKey: []byte{6}, IsInRegion: false, Inclusive: false, Exclusive: false},
		{Key: []byte{4}, StartKey: []byte{3}, EndKey: []byte{6}, IsInRegion: true, Inclusive: true, Exclusive: true},
		{Key: []byte{4}, StartKey: []byte{3}, EndKey: []byte{}, IsInRegion: true, Inclusive: true, Exclusive: true},
		{Key: []byte{3}, StartKey: []byte{3}, EndKey: []byte{}, IsInRegion: true, Inclusive: true, Exclusive: false},
		{Key: []byte{2}, StartKey: []byte{3}, EndKey: []byte{6}, IsInRegion: false, Inclusive: false, Exclusive: false},
		{Key: []byte{}, StartKey: []byte{3}, EndKey: []byte{6}, IsInRegion: false, Inclusive: false, Exclusive: false},
		{Key: []byte{}, StartKey: []byte{3}, EndKey: []byte{}, IsInRegion: false, Inclusive: false, Exclusive: false},
		{Key: []byte{6}, StartKey: []byte{3}, EndKey: []byte{6}, IsInRegion: false, Inclusive: true, Exclusive: false},
	}
	for _, c := range test_cases {
		region := new(metapb.Region)
		region.StartKey = c.StartKey
		region.EndKey = c.EndKey
		result := CheckKeyInRegion(c.Key, region)
		assert.Equal(t, result == nil, c.IsInRegion)
		result = CheckKeyInRegionInclusive(c.Key, region)
		assert.Equal(t, result == nil, c.Inclusive)
		result = CheckKeyInRegionExclusive(c.Key, region)
		assert.Equal(t, result == nil, c.Exclusive)
	}
}

func TestIsInitialMsg(t *testing.T) {
	type MsgInfo struct {
		MessageType  eraftpb.MessageType
		Commit       uint64
		IsInitialMsg bool
	}
	tbl := []MsgInfo{
		{MessageType: eraftpb.MessageType_MsgRequestVote, Commit: RaftInvalidIndex, IsInitialMsg: true},
		{MessageType: eraftpb.MessageType_MsgRequestPreVote, Commit: RaftInvalidIndex, IsInitialMsg: true},
		{MessageType: eraftpb.MessageType_MsgHeartbeat, Commit: RaftInvalidIndex, IsInitialMsg: true},
		{MessageType: eraftpb.MessageType_MsgHeartbeat, Commit: 100, IsInitialMsg: false},
		{MessageType: eraftpb.MessageType_MsgAppend, Commit: 100, IsInitialMsg: false},
	}
	for _, m := range tbl {
		msg := new(eraftpb.Message)
		msg.MsgType = m.MessageType
		msg.Commit = m.Commit
		assert.Equal(t, isInitialMsg(msg), m.IsInitialMsg)
	}
}

func TestEpochStale(t *testing.T) {
	epoch := new(metapb.RegionEpoch)
	epoch.Version = 10
	epoch.ConfVer = 10

	type Ep struct {
		Version uint64
		ConfVer uint64
		IsStale bool
	}
	tbl := []Ep{
		{Version: 11, ConfVer: 10, IsStale: true},
		{Version: 10, ConfVer: 11, IsStale: true},
		{Version: 10, ConfVer: 10, IsStale: false},
		{Version: 10, ConfVer: 9, IsStale: false},
	}
	for _, e := range tbl {
		checkEpoch := new(metapb.RegionEpoch)
		checkEpoch.Version = e.Version
		checkEpoch.ConfVer = e.ConfVer
		assert.Equal(t, IsEpochStale(epoch, checkEpoch), e.IsStale)
	}
}

func TestCheckRegionEpoch(t *testing.T) {
	epoch := new(metapb.RegionEpoch)
	epoch.ConfVer = 2
	epoch.Version = 2
	region := new(metapb.Region)
	region.RegionEpoch = epoch

	// Epoch is required for most requests even if it's empty.
	emptyReq := new(raft_cmdpb.RaftCmdRequest)
	assert.NotNil(t, CheckRegionEpoch(emptyReq, region, false))

	// These admin commands do not require epoch.
	tys := []raft_cmdpb.AdminCmdType{
		raft_cmdpb.AdminCmdType_CompactLog,
		raft_cmdpb.AdminCmdType_InvalidAdmin,
	}
	for _, ty := range tys {
		admin := new(raft_cmdpb.AdminRequest)
		admin.CmdType = ty
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = admin

		// It is Okay if req does not have region epoch.
		assert.Nil(t, CheckRegionEpoch(req, region, false))

		req.Header = new(raft_cmdpb.RaftRequestHeader)
		req.Header.RegionEpoch = epoch
		assert.Nil(t, CheckRegionEpoch(req, region, true))
		assert.Nil(t, CheckRegionEpoch(req, region, false))
	}

	// These admin commands requires epoch.version.
	tys = []raft_cmdpb.AdminCmdType{
		raft_cmdpb.AdminCmdType_BatchSplit,
		raft_cmdpb.AdminCmdType_TransferLeader,
	}
	for _, ty := range tys {
		admin := new(raft_cmdpb.AdminRequest)
		admin.CmdType = ty
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = admin

		// Error if req does not have region epoch.
		assert.NotNil(t, CheckRegionEpoch(req, region, false))

		staleVersionEpoch := *epoch
		staleVersionEpoch.Version = 1
		staleRegion := new(metapb.Region)
		staleVersionEpochCloned := staleVersionEpoch
		staleRegion.RegionEpoch = &staleVersionEpochCloned
		staleVersionEpochCloned2 := staleVersionEpoch
		req.Header = new(raft_cmdpb.RaftRequestHeader)
		req.Header.RegionEpoch = &staleVersionEpochCloned2
		assert.Nil(t, CheckRegionEpoch(req, staleRegion, false))

		latestVersionEpoch := *epoch
		latestVersionEpoch.Version = 3

		for _, e := range []metapb.RegionEpoch{staleVersionEpoch, latestVersionEpoch} {
			eCloned := e
			req.Header.RegionEpoch = &eCloned
			assert.NotNil(t, CheckRegionEpoch(req, region, false))
			assert.NotNil(t, CheckRegionEpoch(req, region, true))
		}
	}

	// These admin commands requires epoch.conf_version.
	for _, ty := range []raft_cmdpb.AdminCmdType{
		raft_cmdpb.AdminCmdType_BatchSplit,
		raft_cmdpb.AdminCmdType_ChangePeer,
		raft_cmdpb.AdminCmdType_TransferLeader,
	} {
		admin := new(raft_cmdpb.AdminRequest)
		admin.CmdType = ty
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = admin
		req.Header = new(raft_cmdpb.RaftRequestHeader)

		// Error if req does not have region epoch.
		assert.NotNil(t, CheckRegionEpoch(req, region, false))

		staleConfEpoch := cloneEpoch(epoch)
		staleConfEpoch.ConfVer = 1
		staleRegion := new(metapb.Region)
		staleRegion.RegionEpoch = cloneEpoch(staleConfEpoch)
		req.Header.RegionEpoch = cloneEpoch(staleConfEpoch)
		assert.Nil(t, CheckRegionEpoch(req, staleRegion, false))

		latestConfEpoch := cloneEpoch(epoch)
		latestConfEpoch.ConfVer = 3
		for _, e := range []*metapb.RegionEpoch{staleConfEpoch, latestConfEpoch} {
			req.Header.RegionEpoch = cloneEpoch(e)
			assert.NotNil(t, CheckRegionEpoch(req, region, false))
			assert.NotNil(t, CheckRegionEpoch(req, region, true))
		}
	}
}

func cloneEpoch(epoch *metapb.RegionEpoch) *metapb.RegionEpoch {
	return &metapb.RegionEpoch{
		ConfVer: epoch.ConfVer,
		Version: epoch.Version,
	}
}
