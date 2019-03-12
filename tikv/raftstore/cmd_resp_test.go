package raftstore

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdResp(t *testing.T) {
	resp := new(raft_cmdpb.RaftCmdResponse)
	ensureRespHeader(resp)
	require.NotNil(t, resp.Header)

	term := uint64(1)
	BindRespTerm(resp, term)
	assert.Equal(t, resp.Header.CurrentTerm, term)

	regionId := uint64(2)
	notLeader := &ErrNotLeader{RegionId: regionId}
	BindRespError(resp, notLeader)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionId)

	resp = NewRespFromError(notLeader)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionId)

	resp = ErrResp(notLeader, term)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.CurrentTerm, term)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionId)
}
