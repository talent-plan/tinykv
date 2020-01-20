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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// nextEnts returns the appliable entries and updates the applied index
func nextEnts(r *Raft, s *MemoryStorage) (ents []pb.Entry) {
	// Transfer all unstable entries to "stable" storage.
	s.Append(r.RaftLog.unstableEntries())
	r.RaftLog.stableTo(r.RaftLog.LastIndex(), r.RaftLog.lastTerm())

	ents = r.RaftLog.nextEnts()
	r.RaftLog.appliedTo(r.RaftLog.committed)
	return ents
}

func mustAppendEntry(r *Raft, ents ...pb.Entry) {
	if !r.appendEntry(ents...) {
		panic("entry unexpectedly dropped")
	}
}

type stateMachine interface {
	Step(m pb.Message) error
	readMessages() []pb.Message
}

func (r *Raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

func TestProgressBecomeProbe(t *testing.T) {
	match := uint64(1)
	tests := []struct {
		p     *Progress
		wnext uint64
	}{
		{
			&Progress{State: ProgressStateReplicate, Match: match, Next: 5, ins: newInflights(256)},
			2,
		},
		{
			// snapshot finish
			&Progress{State: ProgressStateSnapshot, Match: match, Next: 5, PendingSnapshot: 10, ins: newInflights(256)},
			11,
		},
		{
			// snapshot failure
			&Progress{State: ProgressStateSnapshot, Match: match, Next: 5, PendingSnapshot: 0, ins: newInflights(256)},
			2,
		},
	}
	for i, tt := range tests {
		tt.p.becomeProbe()
		if tt.p.State != ProgressStateProbe {
			t.Errorf("#%d: state = %s, want %s", i, tt.p.State, ProgressStateProbe)
		}
		if tt.p.Match != match {
			t.Errorf("#%d: match = %d, want %d", i, tt.p.Match, match)
		}
		if tt.p.Next != tt.wnext {
			t.Errorf("#%d: next = %d, want %d", i, tt.p.Next, tt.wnext)
		}
	}
}

func TestProgressBecomeReplicate(t *testing.T) {
	p := &Progress{State: ProgressStateProbe, Match: 1, Next: 5, ins: newInflights(256)}
	p.becomeReplicate()

	if p.State != ProgressStateReplicate {
		t.Errorf("state = %s, want %s", p.State, ProgressStateReplicate)
	}
	if p.Match != 1 {
		t.Errorf("match = %d, want 1", p.Match)
	}
	if w := p.Match + 1; p.Next != w {
		t.Errorf("next = %d, want %d", p.Next, w)
	}
}

func TestProgressBecomeSnapshot(t *testing.T) {
	p := &Progress{State: ProgressStateProbe, Match: 1, Next: 5, ins: newInflights(256)}
	p.becomeSnapshot(10)

	if p.State != ProgressStateSnapshot {
		t.Errorf("state = %s, want %s", p.State, ProgressStateSnapshot)
	}
	if p.Match != 1 {
		t.Errorf("match = %d, want 1", p.Match)
	}
	if p.PendingSnapshot != 10 {
		t.Errorf("pendingSnapshot = %d, want 10", p.PendingSnapshot)
	}
}

func TestProgressUpdate(t *testing.T) {
	prevM, prevN := uint64(3), uint64(5)
	tests := []struct {
		update uint64

		wm  uint64
		wn  uint64
		wok bool
	}{
		{prevM - 1, prevM, prevN, false},        // do not decrease match, next
		{prevM, prevM, prevN, false},            // do not decrease next
		{prevM + 1, prevM + 1, prevN, true},     // increase match, do not decrease next
		{prevM + 2, prevM + 2, prevN + 1, true}, // increase match, next
	}
	for i, tt := range tests {
		p := &Progress{
			Match: prevM,
			Next:  prevN,
		}
		ok := p.maybeUpdate(tt.update)
		if ok != tt.wok {
			t.Errorf("#%d: ok= %v, want %v", i, ok, tt.wok)
		}
		if p.Match != tt.wm {
			t.Errorf("#%d: match= %d, want %d", i, p.Match, tt.wm)
		}
		if p.Next != tt.wn {
			t.Errorf("#%d: next= %d, want %d", i, p.Next, tt.wn)
		}
	}
}

func TestProgressMaybeDecr(t *testing.T) {
	tests := []struct {
		state    ProgressStateType
		m        uint64
		n        uint64
		rejected uint64
		last     uint64

		w  bool
		wn uint64
	}{
		{
			// state replicate and rejected is not greater than match
			ProgressStateReplicate, 5, 10, 5, 5, false, 10,
		},
		{
			// state replicate and rejected is not greater than match
			ProgressStateReplicate, 5, 10, 4, 4, false, 10,
		},
		{
			// state replicate and rejected is greater than match
			// directly decrease to match+1
			ProgressStateReplicate, 5, 10, 9, 9, true, 6,
		},
		{
			// next-1 != rejected is always false
			ProgressStateProbe, 0, 0, 0, 0, false, 0,
		},
		{
			// next-1 != rejected is always false
			ProgressStateProbe, 0, 10, 5, 5, false, 10,
		},
		{
			// next>1 = decremented by 1
			ProgressStateProbe, 0, 10, 9, 9, true, 9,
		},
		{
			// next>1 = decremented by 1
			ProgressStateProbe, 0, 2, 1, 1, true, 1,
		},
		{
			// next<=1 = reset to 1
			ProgressStateProbe, 0, 1, 0, 0, true, 1,
		},
		{
			// decrease to min(rejected, last+1)
			ProgressStateProbe, 0, 10, 9, 2, true, 3,
		},
		{
			// rejected < 1, reset to 1
			ProgressStateProbe, 0, 10, 9, 0, true, 1,
		},
	}
	for i, tt := range tests {
		p := &Progress{
			State: tt.state,
			Match: tt.m,
			Next:  tt.n,
		}
		if g := p.maybeDecrTo(tt.rejected, tt.last); g != tt.w {
			t.Errorf("#%d: maybeDecrTo= %t, want %t", i, g, tt.w)
		}
		if gm := p.Match; gm != tt.m {
			t.Errorf("#%d: match= %d, want %d", i, gm, tt.m)
		}
		if gn := p.Next; gn != tt.wn {
			t.Errorf("#%d: next= %d, want %d", i, gn, tt.wn)
		}
	}
}

func TestProgressIsPaused(t *testing.T) {
	tests := []struct {
		state  ProgressStateType
		paused bool

		w bool
	}{
		{ProgressStateProbe, false, false},
		{ProgressStateProbe, true, true},
		{ProgressStateReplicate, false, false},
		{ProgressStateReplicate, true, false},
		{ProgressStateSnapshot, false, true},
		{ProgressStateSnapshot, true, true},
	}
	for i, tt := range tests {
		p := &Progress{
			State:  tt.state,
			Paused: tt.paused,
			ins:    newInflights(256),
		}
		if g := p.IsPaused(); g != tt.w {
			t.Errorf("#%d: paused= %t, want %t", i, g, tt.w)
		}
	}
}

// TestProgressResume ensures that progress.maybeUpdate and progress.maybeDecrTo
// will reset progress.paused.
func TestProgressResume(t *testing.T) {
	p := &Progress{
		Next:   2,
		Paused: true,
	}
	p.maybeDecrTo(1, 1)
	if p.Paused {
		t.Errorf("paused= %v, want false", p.Paused)
	}
	p.Paused = true
	p.maybeUpdate(2)
	if p.Paused {
		t.Errorf("paused= %v, want false", p.Paused)
	}
}

func TestProgressLeader(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.Prs[2].becomeReplicate()

	// Send proposals to r1. The first 5 entries should be appended to the log.
	propMsg := pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("foo")}}}
	for i := 0; i < 5; i++ {
		if pr := r.Prs[r.id]; pr.State != ProgressStateReplicate || pr.Match != uint64(i+1) || pr.Next != pr.Match+1 {
			t.Errorf("unexpected progress %v", pr)
		}
		if err := r.Step(propMsg); err != nil {
			t.Fatalf("proposal resulted in error: %v", err)
		}
	}
}

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
func TestProgressResumeByHeartbeatResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()

	r.Prs[2].Paused = true

	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
	if !r.Prs[2].Paused {
		t.Errorf("paused = %v, want true", r.Prs[2].Paused)
	}

	r.Prs[2].becomeReplicate()
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgHeartbeatResponse})
	if r.Prs[2].Paused {
		t.Errorf("paused = %v, want false", r.Prs[2].Paused)
	}
}

func TestProgressPaused(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	ms := r.readMessages()
	if len(ms) != 1 {
		t.Errorf("len(ms) = %d, want 1", len(ms))
	}
}

func TestProgressFlowControl(t *testing.T) {
	cfg := newTestConfig(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	cfg.MaxInflightMsgs = 3
	cfg.MaxSizePerMsg = 2048
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()

	// Throw away all the messages relating to the initial election.
	r.readMessages()

	// While node 2 is in probe state, propose a bunch of entries.
	r.Prs[2].becomeProbe()
	blob := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 10; i++ {
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: blob}}})
	}

	ms := r.readMessages()
	// First append has two entries: the empty entry to confirm the
	// election, and the first proposal (only one proposal gets sent
	// because we're in probe state).
	if len(ms) != 1 || ms[0].MsgType != pb.MessageType_MsgAppend {
		t.Fatalf("expected 1 MessageType_MsgAppend, got %v", ms)
	}
	if len(ms[0].Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(ms[0].Entries))
	}
	if len(ms[0].Entries[0].Data) != 0 || len(ms[0].Entries[1].Data) != 1000 {
		t.Fatalf("unexpected entry sizes: %v", ms[0].Entries)
	}

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppendResponse, Index: ms[0].Entries[1].Index})
	ms = r.readMessages()
	if len(ms) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(ms))
	}
	for i, m := range ms {
		if m.MsgType != pb.MessageType_MsgAppend {
			t.Errorf("%d: expected MessageType_MsgAppend, got %s", i, m.MsgType)
		}
		if len(m.Entries) != 2 {
			t.Errorf("%d: expected 2 entries, got %d", i, len(m.Entries))
		}
	}

	// Ack all three of those messages together and get the last two
	// messages (containing three entries).
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppendResponse, Index: ms[2].Entries[1].Index})
	ms = r.readMessages()
	if len(ms) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(ms))
	}
	for i, m := range ms {
		if m.MsgType != pb.MessageType_MsgAppend {
			t.Errorf("%d: expected MessageType_MsgAppend, got %s", i, m.MsgType)
		}
	}
	if len(ms[0].Entries) != 2 {
		t.Errorf("%d: expected 2 entries, got %d", 0, len(ms[0].Entries))
	}
	if len(ms[1].Entries) != 1 {
		t.Errorf("%d: expected 1 entry, got %d", 1, len(ms[1].Entries))
	}
}

func TestUncommittedEntryLimit(t *testing.T) {
	// Use a relatively large number of entries here to prevent regression of a
	// bug which computed the size before it was fixed. This test would fail
	// with the bug, either because we'd get dropped proposals earlier than we
	// expect them, or because the final tally ends up nonzero. (At the time of
	// writing, the former).
	const maxEntries = 1024
	testEntry := pb.Entry{Data: []byte("testdata")}
	maxEntrySize := maxEntries * PayloadSize(&testEntry)

	cfg := newTestConfig(1, []uint64{1, 2, 3}, 5, 1, NewMemoryStorage())
	cfg.MaxUncommittedEntriesSize = uint64(maxEntrySize)
	cfg.MaxInflightMsgs = 2 * 1024 // avoid interference
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	if n := r.uncommittedSize; n != 0 {
		t.Fatalf("expected zero uncommitted size, got %d bytes", n)
	}

	// Set the two followers to the replicate state. Commit to tail of log.
	const numFollowers = 2
	r.Prs[2].becomeReplicate()
	r.Prs[3].becomeReplicate()
	r.uncommittedSize = 0

	// Send proposals to r1. The first 5 entries should be appended to the log.
	propMsg := pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{&testEntry}}
	propEnts := make([]pb.Entry, maxEntries)
	for i := 0; i < maxEntries; i++ {
		if err := r.Step(propMsg); err != nil {
			t.Fatalf("proposal resulted in error: %v", err)
		}
		propEnts[i] = testEntry
	}

	// Send one more proposal to r1. It should be rejected.
	if err := r.Step(propMsg); err != ErrProposalDropped {
		t.Fatalf("proposal not dropped: %v", err)
	}

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	ms := r.readMessages()
	if e := maxEntries * numFollowers; len(ms) != e {
		t.Fatalf("expected %d messages, got %d", e, len(ms))
	}
	r.reduceUncommittedSize(propEnts)
	if r.uncommittedSize != 0 {
		t.Fatalf("committed everything, but still tracking %d", r.uncommittedSize)
	}

	// Send a single large proposal to r1. Should be accepted even though it
	// pushes us above the limit because we were beneath it before the proposal.
	propEnts = make([]pb.Entry, 2*maxEntries)
	propEntsLarge := make([]*pb.Entry, 2*maxEntries)
	for i := range propEnts {
		propEnts[i] = testEntry
		propEntsLarge[i] = &testEntry
	}
	propMsgLarge := pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: propEntsLarge}
	if err := r.Step(propMsgLarge); err != nil {
		t.Fatalf("proposal resulted in error: %v", err)
	}

	// Send one more proposal to r1. It should be rejected, again.
	if err := r.Step(propMsg); err != ErrProposalDropped {
		t.Fatalf("proposal not dropped: %v", err)
	}

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	ms = r.readMessages()
	if e := 1 * numFollowers; len(ms) != e {
		t.Fatalf("expected %d messages, got %d", e, len(ms))
	}
	r.reduceUncommittedSize(propEnts)
	if n := r.uncommittedSize; n != 0 {
		t.Fatalf("expected zero uncommitted size, got %d", n)
	}
}

func TestLeaderElection(t *testing.T) {
	var cfg func(*Config)
	candState := StateCandidate
	candTerm := uint64(1)
	tests := []struct {
		*network
		state   StateType
		expTerm uint64
	}{
		{newNetworkWithConfig(cfg, nil, nil, nil), StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nil, nopStepper), StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), StateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfig(cfg,
			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
			StateFollower, 1},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		sm := tt.network.peers[1].(*Raft)
		if sm.State != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.State, tt.state)
		}
		if g := sm.Term; g != tt.expTerm {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.expTerm)
		}
	}
}

// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
func TestLearnerElectionTimeout(t *testing.T) {
	n1 := newTestLearnerRaft(1, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())
	n2 := newTestLearnerRaft(2, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	// n2 is learner. Learner should not start election even when times out.
	setRandomizedElectionTimeout(n2, n2.electionTimeout)
	for i := 0; i < n2.electionTimeout; i++ {
		n2.tick()
	}

	if n2.State != StateFollower {
		t.Errorf("peer 2 state: %s, want %s", n2.State, StateFollower)
	}
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
func TestLearnerPromotion(t *testing.T) {
	n1 := newTestLearnerRaft(1, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())
	n2 := newTestLearnerRaft(2, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	nt := newNetwork(n1, n2)

	if n1.State == StateLeader {
		t.Error("peer 1 state is leader, want not", n1.State)
	}

	// n1 should become leader
	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}

	if n1.State != StateLeader {
		t.Errorf("peer 1 state: %s, want %s", n1.State, StateLeader)
	}
	if n2.State != StateFollower {
		t.Errorf("peer 2 state: %s, want %s", n2.State, StateFollower)
	}

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

	n1.addNode(2)
	n2.addNode(2)
	if n2.IsLearner {
		t.Error("peer 2 is learner, want not")
	}

	// n2 start election, should become leader
	setRandomizedElectionTimeout(n2, n2.electionTimeout)
	for i := 0; i < n2.electionTimeout; i++ {
		n2.tick()
	}

	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgBeat})

	if n1.State != StateFollower {
		t.Errorf("peer 1 state: %s, want %s", n1.State, StateFollower)
	}
	if n2.State != StateLeader {
		t.Errorf("peer 2 state: %s, want %s", n2.State, StateLeader)
	}
}

// TestLearnerCannotVote checks that a learner can't vote even it receives a valid Vote request.
func TestLearnerCannotVote(t *testing.T) {
	n2 := newTestLearnerRaft(2, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())

	n2.becomeFollower(1, None)

	n2.Step(pb.Message{From: 1, To: 2, Term: 2, MsgType: pb.MessageType_MsgRequestVote, LogTerm: 11, Index: 11})

	if len(n2.msgs) != 0 {
		t.Errorf("expect learner not to vote, but received %v messages", n2.msgs)
	}
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
func TestLeaderCycle(t *testing.T) {
	var cfg func(*Config)
	n := newNetworkWithConfig(cfg, nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		n.send(pb.Message{From: campaignerID, To: campaignerID, MsgType: pb.MessageType_MsgHup})

		for _, peer := range n.peers {
			sm := peer.(*Raft)
			if sm.id == campaignerID && sm.State != StateLeader {
				t.Errorf("campaigning node %d state = %v, want StateLeader",
					sm.id, sm.State)
			} else if sm.id != campaignerID && sm.State != StateFollower {
				t.Errorf("after campaign of node %d, "+
					"node %d had state = %v, want StateFollower",
					campaignerID, sm.id, sm.State)
			}
		}
	}
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
func TestLeaderElectionOverwriteNewerLogs(t *testing.T) {
	var cfg func(*Config)
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	n := newNetworkWithConfig(cfg,
		entsWithConfig(cfg, 1),     // Node 1: Won first election
		entsWithConfig(cfg, 1),     // Node 2: Got logs from node 1
		entsWithConfig(cfg, 2),     // Node 3: Won second election
		votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
		votedWithConfig(cfg, 3, 2)) // Node 5: Voted but didn't get logs

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
	n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	sm1 := n.peers[1].(*Raft)
	if sm1.State != StateFollower {
		t.Errorf("state = %s, want StateFollower", sm1.State)
	}
	if sm1.Term != 2 {
		t.Errorf("term = %d, want 2", sm1.Term)
	}

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if sm1.State != StateLeader {
		t.Errorf("state = %s, want StateLeader", sm1.State)
	}
	if sm1.Term != 3 {
		t.Errorf("term = %d, want 3", sm1.Term)
	}

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for i := range n.peers {
		sm := n.peers[i].(*Raft)
		entries := sm.RaftLog.allEntries()
		if len(entries) != 2 {
			t.Fatalf("node %d: len(entries) == %d, want 2", i, len(entries))
		}
		if entries[0].Term != 1 {
			t.Errorf("node %d: term at index 1 == %d, want 1", i, entries[0].Term)
		}
		if entries[1].Term != 3 {
			t.Errorf("node %d: term at index 2 == %d, want 3", i, entries[1].Term)
		}
	}
}

func TestVoteFromAnyState(t *testing.T) {
	vt := pb.MessageType_MsgRequestVote
	vt_resp := pb.MessageType_MsgRequestVoteResponse
	for st := StateType(0); st < numStates; st++ {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Term = 1

		switch st {
		case StateFollower:
			r.becomeFollower(r.Term, 3)
		case StateCandidate:
			r.becomeCandidate()
		case StateLeader:
			r.becomeCandidate()
			r.becomeLeader()
		}

		// Note that setting our state above may have advanced r.Term
		// past its initial value.
		newTerm := r.Term + 1

		msg := pb.Message{
			From:    2,
			To:      1,
			MsgType: vt,
			Term:    newTerm,
			LogTerm: newTerm,
			Index:   42,
		}
		if err := r.Step(msg); err != nil {
			t.Errorf("%s,%s: Step failed: %s", vt, st, err)
		}
		if len(r.msgs) != 1 {
			t.Errorf("%s,%s: %d response messages, want 1: %+v", vt, st, len(r.msgs), r.msgs)
		} else {
			resp := r.msgs[0]
			if resp.MsgType != vt_resp {
				t.Errorf("%s,%s: response message is %s, want %s",
					vt, st, resp.MsgType, vt_resp)
			}
			if resp.Reject {
				t.Errorf("%s,%s: unexpected rejection", vt, st)
			}
		}

		// If this was a vote, we reset our state and term.
		if r.State != StateFollower {
			t.Errorf("%s,%s: state %s, want %s", vt, st, r.State, StateFollower)
		}
		if r.Term != newTerm {
			t.Errorf("%s,%s: term %d, want %d", vt, st, r.Term, newTerm)
		}
		if r.Vote != 2 {
			t.Errorf("%s,%s: vote %d, want 2", vt, st, r.Vote)
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
				{From: 1, To: 2, MsgType: pb.MessageType_MsgHup},
				{From: 1, To: 2, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*Raft)

			if sm.RaftLog.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.RaftLog.committed, tt.wcommitted)
			}

			ents := []pb.Entry{}
			for _, e := range nextEnts(sm, tt.network.storage[j]) {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}
			props := []pb.Message{}
			for _, m := range tt.msgs {
				if m.MsgType == pb.MessageType_MsgPropose {
					props = append(props, m)
				}
			}
			for k, m := range props {
				if !bytes.Equal(ents[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, want %d", i, j, ents[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
func TestLearnerLogReplication(t *testing.T) {
	n1 := newTestLearnerRaft(1, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())
	n2 := newTestLearnerRaft(2, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())

	nt := newNetwork(n1, n2)

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

	// n1 is leader and n2 is learner
	if n1.State != StateLeader {
		t.Errorf("peer 1 state: %s, want %s", n1.State, StateLeader)
	}
	if !n2.IsLearner {
		t.Error("peer 2 state: not learner, want yes")
	}

	nextCommitted := n1.RaftLog.committed + 1
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})
	if n1.RaftLog.committed != nextCommitted {
		t.Errorf("peer 1 wants committed to %d, but still %d", nextCommitted, n1.RaftLog.committed)
	}

	if n1.RaftLog.committed != n2.RaftLog.committed {
		t.Errorf("peer 2 wants committed to %d, but still %d", n1.RaftLog.committed, n2.RaftLog.committed)
	}

	match := n1.getProgress(2).Match
	if match != n2.RaftLog.committed {
		t.Errorf("progress 2 of leader 1 wants match %d, but got %d", n2.RaftLog.committed, match)
	}
}

func TestSingleNodeCommit(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 3)
	}
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 1)
	}

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(pb.MessageType_MsgAppend)

	// elect 2 as the new leader with term 2
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*Raft)
	if sm.RaftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 1)
	}

	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgBeat})
	// append an entry at current term
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	// expect the committed to be advanced
	if sm.RaftLog.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 5)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 1)
	}

	// network recovery
	tt.recover()

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})

	if sm.RaftLog.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 4)
	}
}

func TestDuelingCandidates(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("state = %s, want %s", sm.State, StateLeader)
	}

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = nt.peers[3].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("state = %s, want %s", sm.State, StateCandidate)
	}

	nt.recover()

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	wlog := &RaftLog{
		storage:   &MemoryStorage{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}}},
		committed: 1,
		unstable:  unstable{offset: 2},
	}
	tests := []struct {
		sm      *Raft
		state   StateType
		term    uint64
		raftLog *RaftLog
	}{
		{a, StateFollower, 2, wlog},
		{b, StateFollower, 2, wlog},
		{c, StateFollower, 2, newLog(NewMemoryStorage(), raftLogger)},
	}

	for i, tt := range tests {
		if g := tt.sm.State; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.Term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+uint64(i)].(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	// heal the partition
	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgBeat})

	data := []byte("force follower")
	// send a proposal to 3 to flush out a MessageType_MsgAppend to 1
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: data}}})
	// send heartbeat; flush out commit
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgBeat})

	a := tt.peers[1].(*Raft)
	if g := a.State; g != StateFollower {
		t.Errorf("state = %s, want %s", g, StateFollower)
	}
	if g := a.Term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wantLog := ltoa(&RaftLog{
		storage: &MemoryStorage{
			ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}},
		},
		unstable:  unstable{offset: 3},
		committed: 2,
	})
	for i, p := range tt.peers {
		if sm, ok := p.(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	sm := tt.peers[1].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("state = %d, want %d", sm.State, StateLeader)
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 2, Entries: []*pb.Entry{{Index: 3, Term: 2}}})
	// commit a new entry
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	ilog := &RaftLog{
		storage: &MemoryStorage{
			ents: []pb.Entry{
				{}, {Data: nil, Term: 1, Index: 1},
				{Data: nil, Term: 2, Index: 2}, {Data: nil, Term: 3, Index: 3},
				{Data: []byte("somedata"), Term: 3, Index: 4},
			},
		},
		unstable:  unstable{offset: 5},
		committed: 4,
	}
	base := ltoa(ilog)
	for i, p := range tt.peers {
		if sm, ok := p.(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// TestOldMessagesReply - optimization - reply with new term.

func TestProposal(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for j, tt := range tests {
		send := func(m pb.Message) {
			defer func() {
				// only recover if we expect it to panic (success==false)
				if !tt.success {
					e := recover()
					if e != nil {
						t.Logf("#%d: err: %s", j, e)
					}
				}
			}()
			tt.send(m)
		}

		data := []byte("somedata")

		// promote 1 to become leader
		send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: data}}})

		wantLog := newLog(NewMemoryStorage(), raftLogger)
		if tt.success {
			wantLog = &RaftLog{
				storage: &MemoryStorage{
					ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}},
				},
				unstable:  unstable{offset: 3},
				committed: 2}
		}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*Raft); ok {
				l := ltoa(sm.RaftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.network.peers[1].(*Raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", j, g, 1)
		}
	}
}

func TestProposalByProxy(t *testing.T) {
	data := []byte("somedata")
	tests := []*network{
		newNetwork(nil, nil, nil),
		newNetwork(nil, nil, nopStepper),
	}

	for j, tt := range tests {
		// promote 0 the leader
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

		// propose via follower
		tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

		wantLog := &RaftLog{
			storage: &MemoryStorage{
				ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Data: data, Index: 2}},
			},
			unstable:  unstable{offset: 3},
			committed: 2}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*Raft); ok {
				l := ltoa(sm.RaftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.peers[1].(*Raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", j, g, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []uint64
		logs    []pb.Entry
		smTerm  uint64
		w       uint64
	}{
		// single
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 1}}, 1, 1},
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 1}}, 2, 0},
		{[]uint64{2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 2}}, 2, 1},

		// odd
		{[]uint64{2, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},

		// even
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt.logs)
		storage.hardState = pb.HardState{Term: tt.smTerm}

		sm := newTestRaft(1, []uint64{1}, 10, 2, storage)
		for j := 0; j < len(tt.matches); j++ {
			sm.setProgress(uint64(j)+1, tt.matches[j], tt.matches[j]+1, false)
		}
		sm.maybeCommit()
		if g := sm.RaftLog.committed; g != tt.w {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.w)
		}
	}
}

func TestPastElectionTimeout(t *testing.T) {
	tests := []struct {
		elapse       int
		wprobability float64
		round        bool
	}{
		{5, 0, false},
		{10, 0.1, true},
		{13, 0.4, true},
		{15, 0.6, true},
		{18, 0.9, true},
		{20, 1, false},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
		sm.electionElapsed = tt.elapse
		c := 0
		for j := 0; j < 10000; j++ {
			sm.resetRandomizedElectionTimeout()
			if sm.pastElectionTimeout() {
				c++
			}
		}
		got := float64(c) / 10000.0
		if tt.round {
			got = math.Floor(got*10+0.5) / 10.0
		}
		if got != tt.wprobability {
			t.Errorf("#%d: probability = %v, want %v", i, got, tt.wprobability)
		}
	}
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *Raft, m pb.Message) error {
		called = true
		return nil
	}
	sm := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	sm.step = fakeStep
	sm.Term = 2
	sm.Step(pb.Message{MsgType: pb.MessageType_MsgAppend, Term: sm.Term - 1})
	if called {
		t.Errorf("stepFunc called = %v , want %v", called, false)
	}
}

// TestHandleMessageType_MsgAppend ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMessageType_MsgAppend(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// Ensure 1
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []*pb.Entry{{Index: 1, Term: 2}}}, 1, 1, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []*pb.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []*pb.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []*pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 1, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                            // match entry 1, commit up to last new entry 1
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 1, LogTerm: 1, Index: 1, Commit: 3, Entries: []*pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3}, 2, 2, false},                                            // match entry 2, commit up to last new entry 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, false},                                            // commit up to log.last()
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
		sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
		sm.becomeFollower(2, None)

		sm.handleAppendEntries(tt.m)
		if sm.RaftLog.LastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.RaftLog.LastIndex(), tt.wIndex)
		}
		if sm.RaftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.RaftLog.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Reject != tt.wReject {
			t.Errorf("#%d: reject = %v, want %v", i, m[0].Reject, tt.wReject)
		}
	}
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
func TestHandleHeartbeat(t *testing.T) {
	commit := uint64(2)
	tests := []struct {
		m       pb.Message
		wCommit uint64
	}{
		{pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgHeartbeat, Term: 2, Commit: commit + 1}, commit + 1},
		{pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgHeartbeat, Term: 2, Commit: commit - 1}, commit}, // do not decrease commit
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}})
		sm := newTestRaft(1, []uint64{1, 2}, 5, 1, storage)
		sm.becomeFollower(2, 2)
		sm.RaftLog.commitTo(commit)
		sm.handleHeartbeat(tt.m)
		if sm.RaftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.RaftLog.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].MsgType != pb.MessageType_MsgHeartbeatResponse {
			t.Errorf("#%d: type = %v, want MessageType_MsgHeartbeatResponse", i, m[0].MsgType)
		}
	}
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
func TestHandleHeartbeatResp(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}})
	sm := newTestRaft(1, []uint64{1, 2}, 5, 1, storage)
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.RaftLog.commitTo(sm.RaftLog.LastIndex())

	// A heartbeat response from a node that is behind; re-send MessageType_MsgAppend
	sm.Step(pb.Message{From: 2, MsgType: pb.MessageType_MsgHeartbeatResponse})
	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].MsgType != pb.MessageType_MsgAppend {
		t.Errorf("type = %v, want MessageType_MsgAppend", msgs[0].MsgType)
	}

	// A second heartbeat response generates another MessageType_MsgAppend re-send
	sm.Step(pb.Message{From: 2, MsgType: pb.MessageType_MsgHeartbeatResponse})
	msgs = sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].MsgType != pb.MessageType_MsgAppend {
		t.Errorf("type = %v, want MessageType_MsgAppend", msgs[0].MsgType)
	}

	// Once we have an MessageType_MsgAppendResponse, heartbeats no longer send MessageType_MsgAppend.
	sm.Step(pb.Message{
		From:    2,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   msgs[0].Index + uint64(len(msgs[0].Entries)),
	})
	// Consume the message sent in response to MessageType_MsgAppendResponse
	sm.readMessages()

	sm.Step(pb.Message{From: 2, MsgType: pb.MessageType_MsgHeartbeatResponse})
	msgs = sm.readMessages()
	if len(msgs) != 0 {
		t.Fatalf("len(msgs) = %d, want 0: %+v", len(msgs), msgs)
	}
}

// TestMessageType_MsgAppendResponseWaitReset verifies the resume behavior of a leader
// MessageType_MsgAppendResponse.
func TestMessageType_MsgAppendResponseWaitReset(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewMemoryStorage())
	sm.becomeCandidate()
	sm.becomeLeader()

	// The new leader has just emitted a new Term 4 entry; consume those messages
	// from the outgoing queue.
	sm.bcastAppend()
	sm.readMessages()

	// Node 2 acks the first entry, making it committed.
	sm.Step(pb.Message{
		From:    2,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   1,
	})
	if sm.RaftLog.committed != 1 {
		t.Fatalf("expected committed to be 1, got %d", sm.RaftLog.committed)
	}
	// Also consume the MessageType_MsgAppend messages that update Commit on the followers.
	sm.readMessages()

	// A new command is now proposed on node 1.
	sm.Step(pb.Message{
		From:    1,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})

	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MessageType_MsgAppendResponse, but node 3 is still waiting.
	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].MsgType != pb.MessageType_MsgAppend || msgs[0].To != 2 {
		t.Errorf("expected MessageType_MsgAppend to node 2, got %v to %d", msgs[0].MsgType, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
	sm.Step(pb.Message{
		From:    3,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   1,
	})
	msgs = sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].MsgType != pb.MessageType_MsgAppend || msgs[0].To != 3 {
		t.Errorf("expected MessageType_MsgAppend to node 3, got %v to %d", msgs[0].MsgType, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}
}

func TestRecvMessageType_MsgRequestVote(t *testing.T) {
	msgType := pb.MessageType_MsgRequestVote
	msgRespType := pb.MessageType_MsgRequestVoteResponse
	tests := []struct {
		state          StateType
		index, logTerm uint64
		voteFor        uint64
		wreject        bool
	}{
		{StateFollower, 0, 0, None, true},
		{StateFollower, 0, 1, None, true},
		{StateFollower, 0, 2, None, true},
		{StateFollower, 0, 3, None, false},

		{StateFollower, 1, 0, None, true},
		{StateFollower, 1, 1, None, true},
		{StateFollower, 1, 2, None, true},
		{StateFollower, 1, 3, None, false},

		{StateFollower, 2, 0, None, true},
		{StateFollower, 2, 1, None, true},
		{StateFollower, 2, 2, None, false},
		{StateFollower, 2, 3, None, false},

		{StateFollower, 3, 0, None, true},
		{StateFollower, 3, 1, None, true},
		{StateFollower, 3, 2, None, false},
		{StateFollower, 3, 3, None, false},

		{StateFollower, 3, 2, 2, false},
		{StateFollower, 3, 2, 1, true},

		{StateLeader, 3, 3, 1, true},
		{StateCandidate, 3, 3, 1, true},
	}

	max := func(a, b uint64) uint64 {
		if a > b {
			return a
		}
		return b
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
		sm.State = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.Vote = tt.voteFor
		sm.RaftLog = &RaftLog{
			storage:  &MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 2}, {Index: 2, Term: 2}}},
			unstable: unstable{offset: 3},
		}

		// raft.Term is greater than or equal to raft.RaftLog.lastTerm. In this
		// test we're only testing MessageType_MsgRequestVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		term := max(sm.RaftLog.lastTerm(), tt.logTerm)
		sm.Term = term
		sm.Step(pb.Message{MsgType: msgType, Term: term, From: 2, Index: tt.index, LogTerm: tt.logTerm})

		msgs := sm.readMessages()
		if g := len(msgs); g != 1 {
			t.Fatalf("#%d: len(msgs) = %d, want 1", i, g)
			continue
		}
		if g := msgs[0].MsgType; g != msgRespType {
			t.Errorf("#%d, m.MsgType = %v, want %v", i, g, msgRespType)
		}
		if g := msgs[0].Reject; g != tt.wreject {
			t.Errorf("#%d, m.Reject = %v, want %v", i, g, tt.wreject)
		}
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   StateType
		to     StateType
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{StateFollower, StateFollower, true, 1, None},
		{StateFollower, StateCandidate, true, 1, None},
		{StateFollower, StateLeader, false, 0, None},

		{StateCandidate, StateFollower, true, 0, None},
		{StateCandidate, StateCandidate, true, 1, None},
		{StateCandidate, StateLeader, true, 0, 1},

		{StateLeader, StateFollower, true, 1, None},
		{StateLeader, StateCandidate, false, 1, None},
		{StateLeader, StateLeader, true, 0, 1},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			sm := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
			sm.State = tt.from

			switch tt.to {
			case StateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case StateCandidate:
				sm.becomeCandidate()
			case StateLeader:
				sm.becomeLeader()
			}

			if sm.Term != tt.wterm {
				t.Errorf("%d: term = %d, want %d", i, sm.Term, tt.wterm)
			}
			if sm.Lead != tt.wlead {
				t.Errorf("%d: lead = %d, want %d", i, sm.Lead, tt.wlead)
			}
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state StateType

		wstate StateType
		wterm  uint64
		windex uint64
	}{
		{StateFollower, StateFollower, 3, 0},
		{StateCandidate, StateFollower, 3, 0},
		{StateLeader, StateFollower, 3, 1},
	}

	tmsgTypes := [...]pb.MessageType{pb.MessageType_MsgRequestVote, pb.MessageType_MsgAppend}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		switch tt.state {
		case StateFollower:
			sm.becomeFollower(1, None)
		case StateCandidate:
			sm.becomeCandidate()
		case StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, MsgType: msgType, Term: tterm, LogTerm: tterm})

			if sm.State != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.State, tt.wstate)
			}
			if sm.Term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.Term, tt.wterm)
			}
			if sm.RaftLog.LastIndex() != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, sm.RaftLog.LastIndex(), tt.windex)
			}
			if uint64(len(sm.RaftLog.allEntries())) != tt.windex {
				t.Errorf("#%d.%d len(ents) = %v , want %v", i, j, len(sm.RaftLog.allEntries()), tt.windex)
			}
			wlead := uint64(2)
			if msgType == pb.MessageType_MsgRequestVote {
				wlead = None
			}
			if sm.Lead != wlead {
				t.Errorf("#%d, sm.Lead = %d, want %d", i, sm.Lead, None)
			}
		}
	}
}

func TestCandidateResetTermMessageType_MsgHeartbeat(t *testing.T) {
	testCandidateResetTerm(t, pb.MessageType_MsgHeartbeat)
}

func TestCandidateResetTermMessageType_MsgAppend(t *testing.T) {
	testCandidateResetTerm(t, pb.MessageType_MsgAppend)
}

// testCandidateResetTerm tests when a candidate receives a
// MessageType_MsgHeartbeat or MessageType_MsgAppend from leader, "Step" resets the term
// with leader's and reverts back to follower.
func testCandidateResetTerm(t *testing.T, mt pb.MessageType) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	nt := newNetwork(a, b, c)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}
	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}
	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	// isolate 3 and increase term in rest
	nt.isolate(3)

	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}
	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}

	// trigger campaign in isolated c
	c.resetRandomizedElectionTimeout()
	for i := 0; i < c.randomizedElectionTimeout; i++ {
		c.tick()
	}

	if c.State != StateCandidate {
		t.Errorf("state = %s, want %s", c.State, StateCandidate)
	}

	nt.recover()

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
	nt.send(pb.Message{From: 1, To: 3, Term: a.Term, MsgType: mt})

	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	// follower c term is reset with leader's
	if a.Term != c.Term {
		t.Errorf("follower term expected same term as leader's %d, got %d", a.Term, c.Term)
	}
}

func TestLeaderStepdownWhenQuorumActive(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewMemoryStorage())

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.electionTimeout+1; i++ {
		sm.Step(pb.Message{From: 2, MsgType: pb.MessageType_MsgHeartbeatResponse, Term: sm.Term})
		sm.tick()
	}

	if sm.State != StateLeader {
		t.Errorf("state = %v, want %v", sm.State, StateLeader)
	}
}

func TestLeaderStepdownWhenQuorumLost(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewMemoryStorage())

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.electionTimeout+1; i++ {
		sm.tick()
	}

	if sm.State != StateFollower {
		t.Errorf("state = %v, want %v", sm.State, StateFollower)
	}
}

func TestLeaderSupersedingWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}

	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	// Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
	if c.State != StateCandidate {
		t.Errorf("state = %s, want %s", c.State, StateCandidate)
	}

	// Letting b's electionElapsed reach to electionTimeout
	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	if c.State != StateLeader {
		t.Errorf("state = %s, want %s", c.State, StateLeader)
	}
}

func TestLeaderElectionWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}

	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)
	for i := 0; i < a.electionTimeout; i++ {
		a.tick()
	}
	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	if a.State != StateFollower {
		t.Errorf("state = %s, want %s", a.State, StateFollower)
	}

	if c.State != StateLeader {
		t.Errorf("state = %s, want %s", c.State, StateLeader)
	}
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
func TestFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(1)
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}

	if c.State != StateCandidate {
		t.Errorf("state = %s, want %s", c.State, StateCandidate)
	}

	if c.Term != b.Term+1 {
		t.Errorf("term = %d, want %d", c.Term, b.Term+1)
	}

	// Vote again for safety
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}

	if c.State != StateCandidate {
		t.Errorf("state = %s, want %s", c.State, StateCandidate)
	}

	if c.Term != b.Term+2 {
		t.Errorf("term = %d, want %d", c.Term, b.Term+2)
	}

	nt.recover()
	nt.send(pb.Message{From: 1, To: 3, MsgType: pb.MessageType_MsgHeartbeat, Term: a.Term})

	// Disrupt the leader so that the stuck peer is freed
	if a.State != StateFollower {
		t.Errorf("state = %s, want %s", a.State, StateFollower)
	}

	if c.Term != a.Term {
		t.Errorf("term = %d, want %d", c.Term, a.Term)
	}

	// Vote again, should become leader this time
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	if c.State != StateLeader {
		t.Errorf("peer 3 state: %s, want %s", c.State, StateLeader)
	}
}

func TestNonPromotableVoterWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1}, 10, 1, NewMemoryStorage())

	a.checkQuorum = true
	b.checkQuorum = true

	nt := newNetwork(a, b)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b.delProgress(2)

	if b.promotable() {
		t.Fatalf("promotable = %v, want false", b.promotable())
	}

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}

	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}

	if b.Lead != 1 {
		t.Errorf("lead = %d, want 1", b.Lead)
	}
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
func TestDisruptiveFollower(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// check state
	// n1.State == StateLeader
	// n2.State == StateFollower
	// n3.State == StateFollower
	if n1.State != StateLeader {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateLeader)
	}
	if n2.State != StateFollower {
		t.Fatalf("node 2 state: %s, want %s", n2.State, StateFollower)
	}
	if n3.State != StateFollower {
		t.Fatalf("node 3 state: %s, want %s", n3.State, StateFollower)
	}

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	setRandomizedElectionTimeout(n3, n3.electionTimeout+2)
	for i := 0; i < n3.randomizedElectionTimeout-1; i++ {
		n3.tick()
	}

	// ideally, before last election tick elapses,
	// the follower n3 receives "pb.MessageType_MsgAppend" or "pb.MessageType_MsgHeartbeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
	n3.tick()

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	// n1.State == StateLeader
	// n2.State == StateFollower
	// n3.State == StateCandidate
	if n1.State != StateLeader {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateLeader)
	}
	if n2.State != StateFollower {
		t.Fatalf("node 2 state: %s, want %s", n2.State, StateFollower)
	}
	if n3.State != StateCandidate {
		t.Fatalf("node 3 state: %s, want %s", n3.State, StateCandidate)
	}
	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 3
	if n1.Term != 2 {
		t.Fatalf("node 1 term: %d, want %d", n1.Term, 2)
	}
	if n2.Term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.Term, 2)
	}
	if n3.Term != 3 {
		t.Fatalf("node 3 term: %d, want %d", n3.Term, 3)
	}

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	nt.send(pb.Message{From: 1, To: 3, Term: n1.Term, MsgType: pb.MessageType_MsgHeartbeat})

	// then candidate n3 responds with "pb.MessageType_MsgAppendResponse" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	// n1.State == StateFollower
	// n2.State == StateFollower
	// n3.State == StateCandidate
	if n1.State != StateFollower {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateFollower)
	}
	if n2.State != StateFollower {
		t.Fatalf("node 2 state: %s, want %s", n2.State, StateFollower)
	}
	if n3.State != StateCandidate {
		t.Fatalf("node 3 state: %s, want %s", n3.State, StateCandidate)
	}
	// check term
	// n1.Term == 3
	// n2.Term == 2
	// n3.Term == 3
	if n1.Term != 3 {
		t.Fatalf("node 1 term: %d, want %d", n1.Term, 3)
	}
	if n2.Term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.Term, 2)
	}
	if n3.Term != 3 {
		t.Fatalf("node 3 term: %d, want %d", n3.Term, 3)
	}
}

func TestLeaderAppResp(t *testing.T) {
	// initial progress: match = 0; next = 3
	tests := []struct {
		index  uint64
		reject bool
		// progress
		wmatch uint64
		wnext  uint64
		// message
		wmsgNum    int
		windex     uint64
		wcommitted uint64
	}{
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit; decrease next and send probing msg
		{2, false, 2, 4, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		{0, false, 0, 3, 0, 0, 0}, // ignore heartbeat replies
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm.RaftLog = &RaftLog{
			storage:  &MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}}},
			unstable: unstable{offset: 3},
		}
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.readMessages()
		sm.Step(pb.Message{From: 2, MsgType: pb.MessageType_MsgAppendResponse, Index: tt.index, Term: sm.Term, Reject: tt.reject, RejectHint: tt.index})

		p := sm.Prs[2]
		if p.Match != tt.wmatch {
			t.Errorf("#%d match = %d, want %d", i, p.Match, tt.wmatch)
		}
		if p.Next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.Next, tt.wnext)
		}

		msgs := sm.readMessages()

		if len(msgs) != tt.wmsgNum {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wmsgNum)
		}
		for j, msg := range msgs {
			if msg.Index != tt.windex {
				t.Errorf("#%d.%d index = %d, want %d", i, j, msg.Index, tt.windex)
			}
			if msg.Commit != tt.wcommitted {
				t.Errorf("#%d.%d commit = %d, want %d", i, j, msg.Commit, tt.wcommitted)
			}
		}
	}
}

// When the leader receives a heartbeat tick, it should
// send a MessageType_MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
func TestBcastBeat(t *testing.T) {
	offset := uint64(1000)
	// make a state machine with log.offset = 1000
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     offset,
			Term:      1,
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(s)
	sm := newTestRaft(1, nil, 10, 1, storage)
	sm.Term = 1

	sm.becomeCandidate()
	sm.becomeLeader()
	for i := 0; i < 10; i++ {
		mustAppendEntry(sm, pb.Entry{Index: uint64(i) + 1})
	}
	// slow follower
	sm.Prs[2].Match, sm.Prs[2].Next = 5, 6
	// normal follower
	sm.Prs[3].Match, sm.Prs[3].Next = sm.RaftLog.LastIndex(), sm.RaftLog.LastIndex()+1

	sm.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	msgs := sm.readMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %v, want 2", len(msgs))
	}
	wantCommitMap := map[uint64]uint64{
		2: min(sm.RaftLog.committed, sm.Prs[2].Match),
		3: min(sm.RaftLog.committed, sm.Prs[3].Match),
	}
	for i, m := range msgs {
		if m.MsgType != pb.MessageType_MsgHeartbeat {
			t.Fatalf("#%d: type = %v, want = %v", i, m.MsgType, pb.MessageType_MsgHeartbeat)
		}
		if m.Index != 0 {
			t.Fatalf("#%d: prevIndex = %d, want %d", i, m.Index, 0)
		}
		if m.LogTerm != 0 {
			t.Fatalf("#%d: prevTerm = %d, want %d", i, m.LogTerm, 0)
		}
		if wantCommitMap[m.To] == 0 {
			t.Fatalf("#%d: unexpected to %d", i, m.To)
		} else {
			if m.Commit != wantCommitMap[m.To] {
				t.Fatalf("#%d: commit = %d, want %d", i, m.Commit, wantCommitMap[m.To])
			}
			delete(wantCommitMap, m.To)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("#%d: len(entries) = %d, want 0", i, len(m.Entries))
		}
	}
}

// tests the output of the state machine when receiving MessageType_MsgBeat
func TestRecvMessageType_MsgBeat(t *testing.T) {
	tests := []struct {
		state StateType
		wMsg  int
	}{
		{StateLeader, 2},
		// candidate and follower should ignore MessageType_MsgBeat
		{StateCandidate, 0},
		{StateFollower, 0},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm.RaftLog = &RaftLog{storage: &MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}}}}
		sm.Term = 1
		sm.State = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

		msgs := sm.readMessages()
		if len(msgs) != tt.wMsg {
			t.Errorf("%d: len(msgs) = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.MsgType != pb.MessageType_MsgHeartbeat {
				t.Errorf("%d: msg.Msgtype = %v, want %v", i, m.MsgType, pb.MessageType_MsgHeartbeat)
			}
		}
	}
}

func TestLeaderIncreaseNext(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	tests := []struct {
		// progress
		state ProgressStateType
		next  uint64

		wnext uint64
	}{
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{ProgressStateReplicate, 2, uint64(len(previousEnts) + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{ProgressStateProbe, 2, 2},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
		sm.RaftLog.append(previousEnts...)
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.Prs[2].State = tt.state
		sm.Prs[2].Next = tt.next
		sm.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

		p := sm.Prs[2]
		if p.Next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.Next, tt.wnext)
		}
	}
}

func TestSendAppendForProgressProbe(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.Prs[2].becomeProbe()

	// each round is a heartbeat
	for i := 0; i < 3; i++ {
		if i == 0 {
			// we expect that raft will only send out one MessageType_MsgAppend on the first
			// loop. After that, the follower is paused until a heartbeat response is
			// received.
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.sendAppend(2)
			msg := r.readMessages()
			if len(msg) != 1 {
				t.Errorf("len(msg) = %d, want %d", len(msg), 1)
			}
			if msg[0].Index != 0 {
				t.Errorf("index = %d, want %d", msg[0].Index, 0)
			}
		}

		if !r.Prs[2].Paused {
			t.Errorf("paused = %v, want true", r.Prs[2].Paused)
		}
		for j := 0; j < 10; j++ {
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.sendAppend(2)
			if l := len(r.readMessages()); l != 0 {
				t.Errorf("len(msg) = %d, want %d", l, 0)
			}
		}

		// do a heartbeat
		for j := 0; j < r.heartbeatTimeout; j++ {
			r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
		}
		if !r.Prs[2].Paused {
			t.Errorf("paused = %v, want true", r.Prs[2].Paused)
		}

		// consume the heartbeat
		msg := r.readMessages()
		if len(msg) != 1 {
			t.Errorf("len(msg) = %d, want %d", len(msg), 1)
		}
		if msg[0].MsgType != pb.MessageType_MsgHeartbeat {
			t.Errorf("type = %v, want %v", msg[0].MsgType, pb.MessageType_MsgHeartbeat)
		}
	}

	// a heartbeat response will allow another message to be sent
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgHeartbeatResponse})
	msg := r.readMessages()
	if len(msg) != 1 {
		t.Errorf("len(msg) = %d, want %d", len(msg), 1)
	}
	if msg[0].Index != 0 {
		t.Errorf("index = %d, want %d", msg[0].Index, 0)
	}
	if !r.Prs[2].Paused {
		t.Errorf("paused = %v, want true", r.Prs[2].Paused)
	}
}

func TestSendAppendForProgressReplicate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.Prs[2].becomeReplicate()

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.sendAppend(2)
		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Errorf("len(msg) = %d, want %d", len(msgs), 1)
		}
	}
}

func TestSendAppendForProgressSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.Prs[2].becomeSnapshot(10)

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.sendAppend(2)
		msgs := r.readMessages()
		if len(msgs) != 0 {
			t.Errorf("len(msg) = %d, want %d", len(msgs), 0)
		}
	}
}

func TestRecvMessageType_MsgUnreachable(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	s := NewMemoryStorage()
	s.Append(previousEnts)
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	// set node 2 to state replicate
	r.Prs[2].Match = 3
	r.Prs[2].becomeReplicate()
	r.Prs[2].optimisticUpdate(5)

	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgUnreachable})

	if r.Prs[2].State != ProgressStateProbe {
		t.Errorf("state = %s, want %s", r.Prs[2].State, ProgressStateProbe)
	}
	if wnext := r.Prs[2].Match + 1; r.Prs[2].Next != wnext {
		t.Errorf("next = %d, want %d", r.Prs[2].Next, wnext)
	}
}

func TestRestore(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}

	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	if ok := sm.restore(s); !ok {
		t.Fatal("restore fail, want succeed")
	}

	if sm.RaftLog.LastIndex() != s.Metadata.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.RaftLog.LastIndex(), s.Metadata.Index)
	}
	if mustTerm(sm.RaftLog.Term(s.Metadata.Index)) != s.Metadata.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.RaftLog.Term(s.Metadata.Index)), s.Metadata.Term)
	}
	sg := sm.nodes()
	if !reflect.DeepEqual(sg, s.Metadata.ConfState.Nodes) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, s.Metadata.ConfState.Nodes)
	}

	if ok := sm.restore(s); ok {
		t.Fatal("restore succeed, want fail")
	}
}

// TestRestoreWithLearner restores a snapshot which contains learners.
func TestRestoreWithLearner(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}, Learners: []uint64{3}},
		},
	}

	storage := NewMemoryStorage()
	sm := newTestLearnerRaft(3, []uint64{1, 2}, []uint64{3}, 8, 2, storage)
	if ok := sm.restore(s); !ok {
		t.Error("restore fail, want succeed")
	}

	if sm.RaftLog.LastIndex() != s.Metadata.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.RaftLog.LastIndex(), s.Metadata.Index)
	}
	if mustTerm(sm.RaftLog.Term(s.Metadata.Index)) != s.Metadata.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.RaftLog.Term(s.Metadata.Index)), s.Metadata.Term)
	}
	sg := sm.nodes()
	if len(sg) != len(s.Metadata.ConfState.Nodes) {
		t.Errorf("sm.Nodes = %+v, length not equal with %+v", sg, s.Metadata.ConfState.Nodes)
	}
	lns := sm.learnerNodes()
	if len(lns) != len(s.Metadata.ConfState.Learners) {
		t.Errorf("sm.LearnerNodes = %+v, length not equal with %+v", sg, s.Metadata.ConfState.Learners)
	}
	for _, n := range s.Metadata.ConfState.Nodes {
		if sm.Prs[n].IsLearner {
			t.Errorf("sm.Node %x isLearner = %s, want %t", n, sm.Prs[n], false)
		}
	}
	for _, n := range s.Metadata.ConfState.Learners {
		if !sm.LearnerPrs[n].IsLearner {
			t.Errorf("sm.Node %x isLearner = %s, want %t", n, sm.Prs[n], true)
		}
	}

	if ok := sm.restore(s); ok {
		t.Error("restore succeed, want fail")
	}
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
func TestRestoreLearnerPromotion(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}

	storage := NewMemoryStorage()
	sm := newTestLearnerRaft(3, []uint64{1, 2}, []uint64{3}, 10, 1, storage)

	if !sm.IsLearner {
		t.Errorf("%x is not learner, want yes", sm.id)
	}

	if ok := sm.restore(s); !ok {
		t.Error("restore fail, want succeed")
	}

	if sm.IsLearner {
		t.Errorf("%x is learner, want not", sm.id)
	}
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
func TestLearnerReceiveSnapshot(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1}, Learners: []uint64{2}},
		},
	}

	n1 := newTestLearnerRaft(1, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())
	n2 := newTestLearnerRaft(2, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())

	n1.restore(s)

	// Force set n1 appplied index.
	n1.RaftLog.appliedTo(n1.RaftLog.committed)

	nt := newNetwork(n1, n2)

	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

	if n2.RaftLog.committed != n1.RaftLog.committed {
		t.Errorf("peer 2 must commit to %d, but %d", n1.RaftLog.committed, n2.RaftLog.committed)
	}
}

func TestRestoreIgnoreSnapshot(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	commit := uint64(1)
	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.RaftLog.append(previousEnts...)
	sm.RaftLog.commitTo(commit)

	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     commit,
			Term:      1,
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}

	// ignore snapshot
	if ok := sm.restore(s); ok {
		t.Errorf("restore = %t, want %t", ok, false)
	}
	if sm.RaftLog.committed != commit {
		t.Errorf("commit = %d, want %d", sm.RaftLog.committed, commit)
	}

	// ignore snapshot and fast forward commit
	s.Metadata.Index = commit + 1
	if ok := sm.restore(s); ok {
		t.Errorf("restore = %t, want %t", ok, false)
	}
	if sm.RaftLog.committed != commit+1 {
		t.Errorf("commit = %d, want %d", sm.RaftLog.committed, commit+1)
	}
}

func TestProvideSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	sm.Prs[2].Next = sm.RaftLog.firstIndex()
	sm.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppendResponse, Index: sm.Prs[2].Next - 1, Reject: true})

	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.MsgType != pb.MessageType_MsgSnapshot {
		t.Errorf("m.MsgType = %v, want %v", m.MsgType, pb.MessageType_MsgSnapshot)
	}
}

func TestIgnoreProvidingSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm.Prs[2].Next = sm.RaftLog.firstIndex() - 1
	sm.Prs[2].RecentActive = false

	sm.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	msgs := sm.readMessages()
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	m := pb.Message{MsgType: pb.MessageType_MsgSnapshot, From: 1, Term: 2, Snapshot: &s}

	sm := newTestRaft(2, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	sm.Step(m)

	if sm.Lead != uint64(1) {
		t.Errorf("sm.Lead = %d, want 1", sm.Lead)
	}

	// TODO(bdarnell): what should this test?
}

func TestSlowNodeRestore(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)
	for j := 0; j <= 100; j++ {
		nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	}
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: lead.nodes()}, nil)
	nt.storage[1].Compact(lead.RaftLog.applied)

	nt.recover()
	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	for {
		nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
		if lead.Prs[3].RecentActive {
			break
		}
	}

	// trigger a snapshot
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	follower := nt.peers[3].(*Raft)

	// trigger a commit
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	if follower.RaftLog.committed != lead.RaftLog.committed {
		t.Errorf("follower.committed = %d, want %d", follower.RaftLog.committed, lead.RaftLog.committed)
	}
}

// TestStepConfig tests that when raft step MessageType_MsgPropose in EntryType_EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
func TestStepConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	index := r.RaftLog.LastIndex()
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{EntryType: pb.EntryType_EntryConfChange}}})
	if g := r.RaftLog.LastIndex(); g != index+1 {
		t.Errorf("index = %d, want %d", g, index+1)
	}
	if r.PendingConfIndex != index+1 {
		t.Errorf("pendingConfIndex = %d, want %d", r.PendingConfIndex, index+1)
	}
}

// TestStepIgnoreConfig tests that if raft step the second MessageType_MsgPropose in
// EntryType_EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
func TestStepIgnoreConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{EntryType: pb.EntryType_EntryConfChange}}})
	index := r.RaftLog.LastIndex()
	pendingConfIndex := r.PendingConfIndex
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{EntryType: pb.EntryType_EntryConfChange}}})
	wents := []pb.Entry{{EntryType: pb.EntryType_EntryNormal, Term: 1, Index: 3, Data: nil}}
	ents, err := r.RaftLog.Entries(index+1, noLimit)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !reflect.DeepEqual(ents, wents) {
		t.Errorf("ents = %+v, want %+v", ents, wents)
	}
	if r.PendingConfIndex != pendingConfIndex {
		t.Errorf("pendingConfIndex = %d, want %d", r.PendingConfIndex, pendingConfIndex)
	}
}

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
func TestNewLeaderPendingConfig(t *testing.T) {
	tests := []struct {
		addEntry      bool
		wpendingIndex uint64
	}{
		{false, 0},
		{true, 1},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
		if tt.addEntry {
			mustAppendEntry(r, pb.Entry{EntryType: pb.EntryType_EntryNormal})
		}
		r.becomeCandidate()
		r.becomeLeader()
		if r.PendingConfIndex != tt.wpendingIndex {
			t.Errorf("#%d: pendingConfIndex = %d, want %d",
				i, r.PendingConfIndex, tt.wpendingIndex)
		}
	}
}

// TestAddNode tests that addNode could update nodes correctly.
func TestAddNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.addNode(2)
	nodes := r.nodes()
	wnodes := []uint64{1, 2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
}

// TestAddLearner tests that addLearner could update nodes correctly.
func TestAddLearner(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.addLearner(2)
	nodes := r.learnerNodes()
	wnodes := []uint64{2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
	if !r.LearnerPrs[2].IsLearner {
		t.Errorf("node 2 is learner %t, want %t", r.Prs[2].IsLearner, true)
	}
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
func TestAddNodeCheckQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.checkQuorum = true

	r.becomeCandidate()
	r.becomeLeader()

	for i := 0; i < r.electionTimeout-1; i++ {
		r.tick()
	}

	r.addNode(2)

	// This tick will reach electionTimeout, which triggers a quorum check.
	r.tick()

	// Node 1 should still be the leader after a single tick.
	if r.State != StateLeader {
		t.Errorf("state = %v, want %v", r.State, StateLeader)
	}

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
	for i := 0; i < r.electionTimeout; i++ {
		r.tick()
	}

	if r.State != StateFollower {
		t.Errorf("state = %v, want %v", r.State, StateFollower)
	}
}

// TestRemoveNode tests that removeNode could update nodes and
// and removed list correctly.
func TestRemoveNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.removeNode(2)
	w := []uint64{1}
	if g := r.nodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	// remove all nodes from cluster
	r.removeNode(1)
	w = []uint64{}
	if g := r.nodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
}

// TestRemoveLearner tests that removeNode could update nodes and
// and removed list correctly.
func TestRemoveLearner(t *testing.T) {
	r := newTestLearnerRaft(1, []uint64{1}, []uint64{2}, 10, 1, NewMemoryStorage())
	r.removeNode(2)
	w := []uint64{1}
	if g := r.nodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	w = []uint64{}
	if g := r.learnerNodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	// remove all nodes from cluster
	r.removeNode(1)
	if g := r.nodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
}
func TestPromotable(t *testing.T) {
	id := uint64(1)
	tests := []struct {
		peers []uint64
		wp    bool
	}{
		{[]uint64{1}, true},
		{[]uint64{1, 2, 3}, true},
		{[]uint64{}, false},
		{[]uint64{2, 3}, false},
	}
	for i, tt := range tests {
		r := newTestRaft(id, tt.peers, 5, 1, NewMemoryStorage())
		if g := r.promotable(); g != tt.wp {
			t.Errorf("#%d: promotable = %v, want %v", i, g, tt.wp)
		}
	}
}

func TestRaftNodes(t *testing.T) {
	tests := []struct {
		ids  []uint64
		wids []uint64
	}{
		{
			[]uint64{1, 2, 3},
			[]uint64{1, 2, 3},
		},
		{
			[]uint64{3, 2, 1},
			[]uint64{1, 2, 3},
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, tt.ids, 10, 1, NewMemoryStorage())
		if !reflect.DeepEqual(r.nodes(), tt.wids) {
			t.Errorf("#%d: nodes = %+v, want %+v", i, r.nodes(), tt.wids)
		}
	}
}

func TestCampaignWhileLeader(t *testing.T) {
	cfg := newTestConfig(1, []uint64{1}, 5, 1, NewMemoryStorage())
	r := newRaft(cfg)
	if r.State != StateFollower {
		t.Errorf("expected new node to be follower but got %s", r.State)
	}
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if r.State != StateLeader {
		t.Errorf("expected single-node election to become leader but got %s", r.State)
	}
	term := r.Term
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if r.State != StateLeader {
		t.Errorf("expected to remain leader but got %s", r.State)
	}
	if r.Term != term {
		t.Errorf("expected to remain in term %v but got %v", term, r.Term)
	}
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
func TestCommitAfterRemoveNode(t *testing.T) {
	// Create a cluster with two nodes.
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, s)
	r.becomeCandidate()
	r.becomeLeader()

	// Begin to remove the second node.
	cc := pb.ConfChange{
		ChangeType: pb.ConfChangeType_RemoveNode,
		NodeId:     2,
	}
	ccData, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{EntryType: pb.EntryType_EntryConfChange, Data: ccData},
		},
	})
	// Stabilize the log and make sure nothing is committed yet.
	if ents := nextEnts(r, s); len(ents) > 0 {
		t.Fatalf("unexpected committed entries: %v", ents)
	}
	ccIndex := r.RaftLog.LastIndex()

	// While the config change is pending, make another proposal.
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{EntryType: pb.EntryType_EntryNormal, Data: []byte("hello")},
		},
	})

	// Node 2 acknowledges the config change, committing it.
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    2,
		Index:   ccIndex,
	})
	ents := nextEnts(r, s)
	if len(ents) != 2 {
		t.Fatalf("expected two committed entries, got %v", ents)
	}
	if ents[0].EntryType != pb.EntryType_EntryNormal || ents[0].Data != nil {
		t.Fatalf("expected ents[0] to be empty, but got %v", ents[0])
	}
	if ents[1].EntryType != pb.EntryType_EntryConfChange {
		t.Fatalf("expected ents[1] to be EntryType_EntryConfChange, got %v", ents[1])
	}

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	r.removeNode(2)
	ents = nextEnts(r, s)
	if len(ents) != 1 || ents[0].EntryType != pb.EntryType_EntryNormal ||
		string(ents[0].Data) != "hello" {
		t.Fatalf("expected one committed EntryType_EntryNormal, got %v", ents)
	}
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
func TestLeaderTransferToUpToDateNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	if lead.Lead != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.Lead)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
func TestLeaderTransferToUpToDateNodeFromFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	if lead.Lead != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.Lead)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
func TestLeaderTransferWithCheckQuorum(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := 1; i < 4; i++ {
		r := nt.peers[uint64(i)].(*Raft)
		r.checkQuorum = true
		setRandomizedElectionTimeout(r, r.electionTimeout+i)
	}

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*Raft)
	for i := 0; i < f.electionTimeout; i++ {
		f.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	if lead.Lead != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.Lead)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToSlowFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.recover()
	lead := nt.peers[1].(*Raft)
	if lead.Prs[3].Match != 1 {
		t.Fatalf("node 1 has match %x for node 3, want %x", lead.Prs[3].Match, 1)
	}

	// Transfer leadership to 3 when node 3 is lack of log.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferAfterSnapshot(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: lead.nodes()}, nil)
	nt.storage[1].Compact(lead.RaftLog.applied)

	nt.recover()
	if lead.Prs[3].Match != 1 {
		t.Fatalf("node 1 has match %x for node 3, want %x", lead.Prs[3].Match, 1)
	}

	// Transfer leadership to 3 when node 3 is lack of snapshot.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	// Send pb.MessageType_MsgHeartbeatResponse to leader to trigger a snapshot for node 3.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgHeartbeatResponse})

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferToSelf(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToNonExistingNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferTimeout(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to isolated node, wait for timeout.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}
	for i := 0; i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	for i := 0; i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferIgnoreProposal(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to isolated node to let transfer pending, then send proposal.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	err := lead.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	if err != ErrProposalDropped {
		t.Fatalf("should return drop proposal error while transferring")
	}

	if lead.Prs[1].Match != 1 {
		t.Fatalf("node 1 has match %x, want %x", lead.Prs[1].Match, 1)
	}
}

func TestLeaderTransferReceiveHigherTermVote(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to isolated node to let transfer pending.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup, Index: 1, Term: 2})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

func TestLeaderTransferRemoveNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.ignore(pb.MessageType_MsgTimeoutNow)

	lead := nt.peers[1].(*Raft)

	// The leadTransferee is removed when leadship transferring.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	lead.removeNode(3)

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
func TestLeaderTransferBack(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	// Transfer leadership back to self.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
func TestLeaderTransferSecondTransferToAnotherNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	// Transfer leadership to another node.
	nt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
func TestLeaderTransferSecondTransferToSameNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	if lead.leadTransferee != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leadTransferee, 3)
	}

	for i := 0; i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	// Second transfer leadership request to the same node.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	for i := 0; i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func checkLeaderTransferState(t *testing.T, r *Raft, state StateType, lead uint64) {
	if r.State != state || r.Lead != lead {
		t.Fatalf("after transferring, node has state %v lead %v, want state %v lead %v", r.State, r.Lead, state, lead)
	}
	if r.leadTransferee != None {
		t.Fatalf("after transferring, node has leadTransferee %v, want leadTransferee %v", r.leadTransferee, None)
	}
}

// TestTransferNonMember verifies that when a MessageType_MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
func TestTransferNonMember(t *testing.T) {
	r := newTestRaft(1, []uint64{2, 3, 4}, 5, 1, NewMemoryStorage())
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTimeoutNow})

	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse})
	r.Step(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse})
	if r.State != StateFollower {
		t.Fatalf("state is %s, want StateFollower", r.State)
	}
}

// TestSplitVote verifies that after split vote, cluster can complete
// election in next round.
func TestSplitVote(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// simulate leader down. followers start split vote.
	nt.isolate(1)
	nt.send([]pb.Message{
		{From: 2, To: 2, MsgType: pb.MessageType_MsgHup},
		{From: 3, To: 3, MsgType: pb.MessageType_MsgHup},
	}...)

	// check whether the term values are expected
	// n2.Term == 3
	// n3.Term == 3
	sm := nt.peers[2].(*Raft)
	if sm.Term != 3 {
		t.Errorf("peer 2 term: %d, want %d", sm.Term, 3)
	}
	sm = nt.peers[3].(*Raft)
	if sm.Term != 3 {
		t.Errorf("peer 3 term: %d, want %d", sm.Term, 3)
	}

	// check state
	// n2 == candidate
	// n3 == candidate
	sm = nt.peers[2].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("peer 2 state: %s, want %s", sm.State, StateCandidate)
	}
	sm = nt.peers[3].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("peer 3 state: %s, want %s", sm.State, StateCandidate)
	}

	// node 2 election timeout first
	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})

	// check whether the term values are expected
	// n2.Term == 4
	// n3.Term == 4
	sm = nt.peers[2].(*Raft)
	if sm.Term != 4 {
		t.Errorf("peer 2 term: %d, want %d", sm.Term, 4)
	}
	sm = nt.peers[3].(*Raft)
	if sm.Term != 4 {
		t.Errorf("peer 3 term: %d, want %d", sm.Term, 4)
	}

	// check state
	// n2 == leader
	// n3 == follower
	sm = nt.peers[2].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("peer 2 state: %s, want %s", sm.State, StateLeader)
	}
	sm = nt.peers[3].(*Raft)
	if sm.State != StateFollower {
		t.Errorf("peer 3 state: %s, want %s", sm.State, StateFollower)
	}
}

func entsWithConfig(configFunc func(*Config), terms ...uint64) *Raft {
	storage := NewMemoryStorage()
	for i, term := range terms {
		storage.Append([]pb.Entry{{Index: uint64(i + 1), Term: term}})
	}
	cfg := newTestConfig(1, []uint64{}, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.reset(terms[len(terms)-1])
	return sm
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
func votedWithConfig(configFunc func(*Config), vote, term uint64) *Raft {
	storage := NewMemoryStorage()
	storage.SetHardState(pb.HardState{Vote: vote, Term: term})
	cfg := newTestConfig(1, []uint64{}, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.reset(term)
	return sm
}

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]*MemoryStorage
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(pb.Message) bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = NewMemoryStorage()
			cfg := newTestConfig(id, peerAddrs, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		case *Raft:
			learners := make(map[uint64]bool, len(v.LearnerPrs))
			for i := range v.LearnerPrs {
				learners[i] = true
			}
			v.id = id
			v.Prs = make(map[uint64]*Progress)
			v.LearnerPrs = make(map[uint64]*Progress)
			for i := 0; i < size; i++ {
				if _, ok := learners[peerAddrs[i]]; ok {
					v.LearnerPrs[peerAddrs[i]] = &Progress{IsLearner: true}
				} else {
					v.Prs[peerAddrs[i]] = &Progress{}
				}
			}
			v.reset(v.Term)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[pb.MessageType]bool),
	}
}

func (nw *network) send(msgs ...pb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0) // always drop
			nw.drop(nid, id, 1.0) // always drop
		}
	}
}

func (nw *network) ignore(t pb.MessageType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[pb.MessageType]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	mm := []pb.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.MsgType] {
			continue
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected MessageType_MsgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		if nw.msgHook != nil {
			if !nw.msgHook(m) {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

func (blackHole) Step(pb.Message) error      { return nil }
func (blackHole) readMessages() []pb.Message { return nil }

var nopStepper = &blackHole{}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
func setRandomizedElectionTimeout(r *Raft, v int) {
	r.randomizedElectionTimeout = v
}

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:              id,
		peers:           peers,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Raft {
	return newRaft(newTestConfig(id, peers, election, heartbeat, storage))
}

func newTestLearnerRaft(id uint64, peers []uint64, learners []uint64, election, heartbeat int, storage Storage) *Raft {
	cfg := newTestConfig(id, peers, election, heartbeat, storage)
	cfg.learners = learners
	return newRaft(cfg)
}
