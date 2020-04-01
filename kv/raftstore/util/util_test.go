package util

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
)

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
		assert.Equal(t, c.IsInRegion, result == nil)
		result = CheckKeyInRegionInclusive(c.Key, region)
		assert.Equal(t, c.Inclusive, result == nil)
		result = CheckKeyInRegionExclusive(c.Key, region)
		assert.Equal(t, c.Exclusive, result == nil)
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
		{MessageType: eraftpb.MessageType_MsgHeartbeat, Commit: RaftInvalidIndex, IsInitialMsg: true},
		{MessageType: eraftpb.MessageType_MsgHeartbeat, Commit: 100, IsInitialMsg: false},
		{MessageType: eraftpb.MessageType_MsgAppend, Commit: 100, IsInitialMsg: false},
	}
	for _, m := range tbl {
		msg := new(eraftpb.Message)
		msg.MsgType = m.MessageType
		msg.Commit = m.Commit
		assert.Equal(t, m.IsInitialMsg, IsInitialMsg(msg))
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
		assert.Equal(t, e.IsStale, IsEpochStale(epoch, checkEpoch))
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
		raft_cmdpb.AdminCmdType_Split,
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
		raft_cmdpb.AdminCmdType_Split,
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
