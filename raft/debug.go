package raft

import (
	"fmt"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func ShowMsg(msg pb.Message) string {
	var entries []string
	for _, e := range msg.Entries {
		entries = append(entries, fmt.Sprintf("{Term: %v, Index: %v}", e.Term, e.Index))
	}
	entriesStr := "[" + strings.Join(entries, ", ") + "]"

	switch msg.MsgType {
	case pb.MessageType_MsgAppend:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v, Entries: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
			entriesStr,
		)
	case pb.MessageType_MsgAppendResponse:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v, Reject: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
			msg.Reject,
		)
	case pb.MessageType_MsgRequestVote:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
		)
	case pb.MessageType_MsgRequestVoteResponse:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v, Reject: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
			msg.Reject,
		)
	case pb.MessageType_MsgHeartbeat:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
		)
	case pb.MessageType_MsgHeartbeatResponse:
		return fmt.Sprintf("{Type: %v, Term: %v, Index: %v, LogTerm: %v, Commit: %v, Reject: %v}",
			pb.MessageType_name[int32(msg.MsgType)],
			msg.Term,
			msg.Index,
			msg.LogTerm,
			msg.Commit,
			msg.Reject,
		)
	}

	return ""
}
