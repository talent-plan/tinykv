// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"strings"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var testFormatter EntryFormatter = func(data []byte) string {
	return strings.ToUpper(string(data))
}

func TestIsLocalMsg(t *testing.T) {
	tests := []struct {
		msgt    pb.MessageType
		isLocal bool
	}{
		{pb.MessageType_MsgHup, true},
		{pb.MessageType_MsgBeat, true},
		{pb.MessageType_MsgSnapStatus, true},
		{pb.MessageType_MsgTransferLeader, false},
		{pb.MessageType_MsgPropose, false},
		{pb.MessageType_MsgAppend, false},
		{pb.MessageType_MsgAppendResponse, false},
		{pb.MessageType_MsgRequestVote, false},
		{pb.MessageType_MsgRequestVoteResponse, false},
		{pb.MessageType_MsgSnapshot, false},
		{pb.MessageType_MsgHeartbeat, false},
		{pb.MessageType_MsgHeartbeatResponse, false},
		{pb.MessageType_MsgTimeoutNow, false},
	}

	for i, tt := range tests {
		got := IsLocalMsg(tt.msgt)
		if got != tt.isLocal {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isLocal)
		}
	}
}
