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
	"math/rand"
	"reflect"
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

type stateMachine interface {
	Step(m pb.Message) error
	readMessages() []pb.Message
}

func (r *Raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

func TestProgressLeader2B(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()

	// Send proposals to r1. The first 5 entries should be appended to the log.
	propMsg := pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("foo")}}}
	for i := 0; i < 5; i++ {
		if pr := r.Prs[r.id]; pr.Match != uint64(i+1) || pr.Next != pr.Match+1 {
			t.Errorf("unexpected progress %v", pr)
		}
		if err := r.Step(propMsg); err != nil {
			t.Fatalf("proposal resulted in error: %v", err)
		}
	}
}

func TestLeaderElection2A(t *testing.T) {
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

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
func TestLeaderCycle2A(t *testing.T) {
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
func TestLeaderElectionOverwriteNewerLogs2B(t *testing.T) {
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

func TestVoteFromAnyState2A(t *testing.T) {
	vt := pb.MessageType_MsgRequestVote
	vt_resp := pb.MessageType_MsgRequestVoteResponse
	for st := StateType(0); st <= StateLeader; st++ {
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

func TestLogReplication2B(t *testing.T) {
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

func TestSingleNodeCommit2B(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 3)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes with noop entry and no new proposal comes in.
func TestCommitWithoutNewTermEntry2B(t *testing.T) {
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

func TestDuelingCandidates2B(t *testing.T) {
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

	wlog := newLog(&MemoryStorage{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}}}, raftLogger)
	wlog.committed = 1
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

func TestCandidateConcede2B(t *testing.T) {
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
	wlog := newLog(&MemoryStorage{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}}, raftLogger)
	wlog.committed = 2
	wantLog := ltoa(wlog)
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

func TestSingleNodeCandidate2A(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	sm := tt.peers[1].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("state = %d, want %d", sm.State, StateLeader)
	}
}

func TestOldMessages2B(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 2, Entries: []*pb.Entry{{Index: 3, Term: 2}}})
	// commit a new entry
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	ilog := newLog(
		&MemoryStorage{
			ents: []pb.Entry{
				{}, {Data: nil, Term: 1, Index: 1},
				{Data: nil, Term: 2, Index: 2}, {Data: nil, Term: 3, Index: 3},
				{Data: []byte("somedata"), Term: 3, Index: 4},
			},
		}, raftLogger)
	ilog.committed = 4
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

func TestProposal2B(t *testing.T) {
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
		data := []byte("somedata")

		// promote 1 to become leader
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: data}}})

		wantLog := newLog(NewMemoryStorage(), raftLogger)
		if tt.success {
			wantLog = newLog(&MemoryStorage{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}}, raftLogger)
			wantLog.committed = 2
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

// TestHandleMessageType_MsgAppend ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMessageType_MsgAppend2B(t *testing.T) {
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
func TestHandleHeartbeat2B(t *testing.T) {
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
func TestHandleHeartbeatResp2B(t *testing.T) {
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

func TestRecvMessageType_MsgRequestVote2A(t *testing.T) {
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
		sm.RaftLog = newLog(&MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 2}, {Index: 2, Term: 2}}}, raftLogger)

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

func TestAllServerStepdown2B(t *testing.T) {
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

func TestCandidateResetTermMessageType_MsgHeartbeat2A(t *testing.T) {
	testCandidateResetTerm(t, pb.MessageType_MsgHeartbeat)
}

func TestCandidateResetTermMessageType_MsgAppend2A(t *testing.T) {
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

	for c.State != StateCandidate {
		c.tick()
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

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
func TestDisruptiveFollower2A(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

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
	for n3.State != StateCandidate {
		n3.tick()
	}

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
	if n1.State != StateFollower {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateFollower)
	}

	// check term
	if n1.Term != 3 {
		t.Fatalf("node 1 term: %d, want %d", n1.Term, 3)
	}
}

func TestLeaderAppResp2B(t *testing.T) {
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
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit;
		{2, false, 2, 3, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		{0, false, 0, 3, 0, 0, 0}, // ignore heartbeat replies
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm.RaftLog = newLog(&MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}}}, raftLogger)
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
func TestBcastBeat2B(t *testing.T) {
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
	sm.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	sm.readMessages() // clear message
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
func TestRecvMessageType_MsgBeat2A(t *testing.T) {
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
		sm.RaftLog = newLog(&MemoryStorage{ents: []pb.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}}}, raftLogger)
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

func TestLeaderIncreaseNext2B(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	// previous entries + noop entry + propose + 1
	wnext := uint64(len(previousEnts)) + 1 + 1 + 1

	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	sm.RaftLog.append(previousEnts...)
	nt := newNetwork(sm, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	p := sm.Prs[2]
	if p.Next != wnext {
		t.Errorf("next = %d, want %d", p.Next, wnext)
	}
}

func TestRestoreSnapshot2B(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}

	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.handleSnapshot(pb.Message{Snapshot: &s})

	if sm.RaftLog.LastIndex() != s.Metadata.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.RaftLog.LastIndex(), s.Metadata.Index)
	}
	if mustTerm(sm.RaftLog.Term(s.Metadata.Index)) != s.Metadata.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.RaftLog.Term(s.Metadata.Index)), s.Metadata.Term)
	}
	sg := nodes(sm)
	if !reflect.DeepEqual(sg, s.Metadata.ConfState.Nodes) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, s.Metadata.ConfState.Nodes)
	}
}

func TestRestoreIgnoreSnapshot2B(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.RaftLog.append(previousEnts...)
	sm.RaftLog.commitTo(3)

	commit := uint64(1)
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     commit,
			Term:      1,
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}

	// ignore snapshot
	sm.handleSnapshot(pb.Message{Snapshot: &s})
	if sm.RaftLog.committed == commit {
		t.Errorf("commit = %d, want %d", sm.RaftLog.committed, commit)
	}
}

func TestProvideSnap2B(t *testing.T) {
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
	sm.handleSnapshot(pb.Message{Snapshot: &s})
	sm.readMessages() // clear message

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

func TestRestoreFromSnapMsg2B(t *testing.T) {
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
}

func TestSlowNodeRestore2B(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)
	for j := 0; j <= 100; j++ {
		nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	}
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: nodes(lead)}, nil)
	nt.storage[1].Compact(lead.RaftLog.applied)

	nt.recover()

	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

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
func TestStepConfig3A(t *testing.T) {
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

// TestAddNode tests that addNode could update nodes correctly.
func TestAddNode3A(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.addNode(2)
	nodes := nodes(r)
	wnodes := []uint64{1, 2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
}

// TestRemoveNode tests that removeNode could update nodes and
// and removed list correctly.
func TestRemoveNode3A(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.removeNode(2)
	w := []uint64{1}
	if g := nodes(r); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	// remove all nodes from cluster
	r.removeNode(1)
	w = []uint64{}
	if g := nodes(r); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
}

func TestCampaignWhileLeader2A(t *testing.T) {
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
func TestCommitAfterRemoveNode3A(t *testing.T) {
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
func TestLeaderTransferToUpToDateNode3C(t *testing.T) {
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
func TestLeaderTransferToUpToDateNodeFromFollower3C(t *testing.T) {
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

func TestLeaderTransferToSlowFollower3C(t *testing.T) {
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

func TestLeaderTransferAfterSnapshot3C(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: nodes(lead)}, nil)
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

func TestLeaderTransferToSelf3C(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToNonExistingNode3C(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferTimeout3C(t *testing.T) {
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

func TestLeaderTransferIgnoreProposal3C(t *testing.T) {
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

func TestLeaderTransferReceiveHigherTermVote3C(t *testing.T) {
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

func TestLeaderTransferRemoveNode3C(t *testing.T) {
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
func TestLeaderTransferBack3C(t *testing.T) {
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
func TestLeaderTransferSecondTransferToAnotherNode3C(t *testing.T) {
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
func TestLeaderTransferSecondTransferToSameNode3C(t *testing.T) {
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
func TestTransferNonMember3C(t *testing.T) {
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
func TestSplitVote2A(t *testing.T) {
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
	sm.Term = terms[len(terms)-1]
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
	sm.Term = term
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
			v.id = id
			v.Prs = make(map[uint64]*Progress)
			for i := 0; i < size; i++ {
				v.Prs[peerAddrs[i]] = &Progress{}
			}
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

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:            id,
		peers:         peers,
		ElectionTick:  election,
		HeartbeatTick: heartbeat,
		Storage:       storage,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Raft {
	return newRaft(newTestConfig(id, peers, election, heartbeat, storage))
}
