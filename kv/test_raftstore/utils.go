package test_raftstore

import (
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func SleepMS(ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func NewPeer(storeID, peerID uint64) metapb.Peer {
	peer := metapb.Peer{}
	peer.StoreId = storeID
	peer.Id = peerID
	return peer
}

func NewBaseRequest(regionID uint64, epoch *metapb.RegionEpoch, readQuorum bool) raft_cmdpb.RaftCmdRequest {
	req := raft_cmdpb.RaftCmdRequest{}
	req.Header = &raft_cmdpb.RaftRequestHeader{RegionId: regionID, RegionEpoch: epoch, ReadQuorum: readQuorum}
	return req
}

func NewRequest(regionID uint64, epoch *metapb.RegionEpoch, requests []*raft_cmdpb.Request, readQuorum bool) raft_cmdpb.RaftCmdRequest {
	req := NewBaseRequest(regionID, epoch, readQuorum)
	req.Requests = requests
	return req
}

func NewPutCfCmd(cf string, key, value []byte) *raft_cmdpb.Request {
	cmd := &raft_cmdpb.Request{}
	cmd.CmdType = raft_cmdpb.CmdType_Put
	cmd.Put = &raft_cmdpb.PutRequest{Key: key, Value: value, Cf: cf}
	return cmd
}

func NewStatusRequest(regionID uint64, peer *metapb.Peer, request *raft_cmdpb.StatusRequest) *raft_cmdpb.RaftCmdRequest {
	req := NewBaseRequest(regionID, &metapb.RegionEpoch{}, false)
	req.Header.Peer = peer
	req.StatusRequest = request
	return &req
}

func NewRegionLeaderCmd() *raft_cmdpb.StatusRequest {
	cmd := raft_cmdpb.StatusRequest{}
	cmd.CmdType = raft_cmdpb.StatusCmdType_RegionLeader
	return &cmd
}
