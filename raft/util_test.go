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
	"math"
	"reflect"
	"strings"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var testFormatter EntryFormatter = func(data []byte) string {
	return strings.ToUpper(string(data))
}

func TestLimitSize(t *testing.T) {
	ents := []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		maxsize  uint64
		wentries []pb.Entry
	}{
		{math.MaxUint64, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{0, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{uint64(ents[0].Size() + ents[1].Size()), []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()/2), []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size() - 1), []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()), []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		if !reflect.DeepEqual(limitSize(ents, tt.maxsize), tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, limitSize(ents, tt.maxsize), tt.wentries)
		}
	}
}

func TestIsLocalMsg(t *testing.T) {
	tests := []struct {
		msgt    pb.MessageType
		isLocal bool
	}{
		{pb.MessageType_MsgHup, true},
		{pb.MessageType_MsgBeat, true},
		{pb.MessageType_MsgUnreachable, true},
		{pb.MessageType_MsgSnapStatus, true},
		{pb.MessageType_MsgCheckQuorum, true},
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
		{pb.MessageType_MsgReadIndex, false},
		{pb.MessageType_MsgReadIndexResp, false},
		{pb.MessageType_MsgRequestPreVote, false},
		{pb.MessageType_MsgRequestPreVoteResponse, false},
	}

	for i, tt := range tests {
		got := IsLocalMsg(tt.msgt)
		if got != tt.isLocal {
			t.Errorf("#%d: got %v, want %v", i, got, tt.isLocal)
		}
	}
}
