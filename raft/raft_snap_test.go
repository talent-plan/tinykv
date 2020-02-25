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
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var (
	testingSnap = pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
)

// TestSnapshotSucceedViaAppResp regression tests the situation in which a snap-
// shot is sent to a follower at the most recent index (i.e. the snapshot index
// is the leader's last index is the committed index). In that situation, a bug
// in the past left the follower in probing status until the next log entry was
// committed.
func TestSnapshotSucceedViaAppResp(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}

	s1 := NewMemoryStorage()
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s1)

	// Become follower because otherwise the way this test sets things up the
	// leadership term will be 1 (which is stale). We want it to match the snap-
	// shot term in this test.
	n1.becomeFollower(snap.Metadata.Term-1, 2)
	n1.becomeCandidate()
	n1.becomeLeader()

	// Apply a snapshot on the leader. This is a workaround against the fact that
	// the leader will always append an empty entry, but that empty entry works
	// against what we're trying to assert in this test, namely that a snapshot
	// at the latest committed index leaves the follower in probing state.
	// With the snapshot, the empty entry is fully committed.
	n1.restore(snap)

	noMessage := pb.MessageType(-1)
	mustSend := func(from, to *Raft, typ pb.MessageType) pb.Message {
		t.Helper()
		for i, msg := range from.msgs {
			if msg.From != from.id || msg.To != to.id || msg.MsgType != typ {
				continue
			}
			if err := to.Step(msg); err != nil {
				t.Fatalf("%v: %s", msg, err)
			}
			from.msgs = append(from.msgs[:i], from.msgs[i+1:]...)
			return msg
		}
		if typ == noMessage {
			if len(from.msgs) == 0 {
				return pb.Message{}
			}
			t.Fatalf("expected no more messages, but got %d->%d %v", from.id, to.id, from.msgs)
		}
		t.Fatalf("message %d->%d %s not found in %v", from.id, to.id, typ, from.msgs)
		return pb.Message{} // unreachable
	}

	// Create the follower that will receive the snapshot.
	s2 := NewMemoryStorage()
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, s2)

	// Let the leader probe the follower.
	if !n1.sendAppend(2) {
		t.Fatalf("expected message to be sent")
	}
	if msg := mustSend(n1, n2, pb.MessageType_MsgAppend); len(msg.Entries) > 0 {
		// For this test to work, the leader must not have anything to append
		// to the follower right now.
		t.Fatalf("unexpectedly appending entries %v", msg.Entries)
	}

	// Follower rejects the append (because it doesn't have any log entries)
	if msg := mustSend(n2, n1, pb.MessageType_MsgAppendResponse); !msg.Reject {
		t.Fatalf("expected a rejection with zero hint, got reject=%t hint=%d", msg.Reject, msg.RejectHint)
	}

	expIdx := snap.Metadata.Index
	// Leader sends snapshot due to RejectHint of zero (the storage we use here
	// has index zero compacted).
	if msg := mustSend(n1, n2, pb.MessageType_MsgSnapshot); msg.Snapshot.Metadata.Index != expIdx {
		t.Fatalf("expected snapshot at index %d, got %d", expIdx, msg.Snapshot.Metadata.Index)
	}

	// n2 reacts to snapshot with MessageType_MsgAppendResponse.
	if msg := mustSend(n2, n1, pb.MessageType_MsgAppendResponse); msg.Index != expIdx {
		t.Fatalf("expected AppResp at index %d, got %d", expIdx, msg.Index)
	}

	// Leader has correct state for follower.
	pr := n1.Prs[2]
	if pr.Match != expIdx || pr.Next != expIdx+1 {
		t.Fatalf("expected match = %d, next = %d; got match = %d and next = %d", expIdx, expIdx+1, pr.Match, pr.Next)
	}

	// Leader and follower are done.
	mustSend(n1, n2, noMessage)
	mustSend(n2, n1, noMessage)
}
