package raftstore

import (
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

func ensureRespHeader(resp *raft_cmdpb.RaftCmdResponse) {
	header := resp.GetHeader()
	if header == nil {
		resp.Header = &raft_cmdpb.RaftResponseHeader{}
	}
}

func BindRespTerm(resp *raft_cmdpb.RaftCmdResponse, term uint64) {
	if term == 0 {
		return
	}
	ensureRespHeader(resp)
	resp.Header.CurrentTerm = term
}

func BindRespError(resp *raft_cmdpb.RaftCmdResponse, err error) {
	ensureRespHeader(resp)
	resp.Header.Error = RaftstoreErrToPbError(err)
}

func ErrResp(err error) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	BindRespError(resp, err)
	return resp
}

func ErrRespWithTerm(err error, term uint64) *raft_cmdpb.RaftCmdResponse {
	resp := ErrResp(err)
	BindRespTerm(resp, term)
	return resp
}

func ErrRespRegionNotFound(regionID uint64) *raft_cmdpb.RaftCmdResponse {
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Error: &errorpb.Error{
				Message: "region is not found",
				RegionNotFound: &errorpb.RegionNotFound{
					RegionId: regionID,
				},
			},
		},
	}
}
