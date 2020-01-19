package message

import (
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type RaftRouter interface {
	Send(regionID uint64, msg Msg)
	SendRaftMessage(msg *raft_serverpb.RaftMessage)
	SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *Callback) error
	ReportSnapshotStatus(regionID uint64, toPeerID uint64, status raft.SnapshotStatus) error
	ReportUnreachable(regionID, toPeerID uint64) error
}
