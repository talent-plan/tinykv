package raftstore

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
)

func TestGetSyncLogFromRequest(t *testing.T) {
	allTypes := map[raft_cmdpb.AdminCmdType]bool{
		raft_cmdpb.AdminCmdType_InvalidAdmin:   false,
		raft_cmdpb.AdminCmdType_ChangePeer:     true,
		raft_cmdpb.AdminCmdType_CompactLog:     false,
		raft_cmdpb.AdminCmdType_TransferLeader: false,
		raft_cmdpb.AdminCmdType_BatchSplit:     true,
	}

	for tp, sync := range allTypes {
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = new(raft_cmdpb.AdminRequest)
		req.AdminRequest.CmdType = tp

		assert.Equal(t, getSyncLogFromRequest(req), sync)
	}
}

func TestEntryCtx(t *testing.T) {
	tbl := [][]ProposalContext{
		{ProposalContext_Split},
		{ProposalContext_SyncLog},
		{ProposalContext_Split, ProposalContext_SyncLog},
	}
	for _, flags := range tbl {
		var ctx ProposalContext
		for _, f := range flags {
			ctx.insert(f)
		}

		ser := ctx.ToBytes()
		de := NewProposalContextFromBytes(ser)

		for _, f := range flags {
			assert.True(t, de.contains(f))
		}
	}
}
