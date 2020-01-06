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
	"reflect"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// TestRawNodeStep ensures that RawNode.Step ignore local message.
func TestRawNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		s := NewMemoryStorage()
		rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s), []Peer{{ID: 1}})
		if err != nil {
			t.Fatal(err)
		}
		msgt := pb.MessageType(i)
		var snap *pb.Snapshot
		if msgt == pb.MessageType_MsgSnapshot {
			snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{}}}
		}
		err = rawNode.Step(pb.Message{MsgType: msgt, Snapshot: snap})
		// LocalMsg should be ignored.
		if IsLocalMsg(msgt) {
			if err != ErrStepLocalMsg {
				t.Errorf("%d: step should ignore %s", msgt, msgn)
			}
		}
	}
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
func TestRawNodeProposeAndConfChange(t *testing.T) {
	s := NewMemoryStorage()
	var err error
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s), []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	if d := rawNode.Ready(); d.MustSync || !IsEmptyHardState(d.HardState) || len(d.Entries) > 0 {
		t.Fatalf("expected empty hard state with must-sync=false: %#v", d)
	}

	rawNode.Campaign()
	proposed := false
	var (
		lastIndex uint64
		ccdata    []byte
	)
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		// Once we are the leader, propose a command and a ConfChange.
		if !proposed && rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Propose([]byte(""), []byte("somedata"))

			cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
			ccdata, err = cc.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			rawNode.ProposeConfChange([]byte(""), cc)

			proposed = true
		}
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}

		// Exit when we have four entries: one ConfChange, one no-op for the election,
		// our proposed command and proposed ConfChange.
		lastIndex, err = s.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if lastIndex >= 4 {
			break
		}
	}

	entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 2)
	}
	if !bytes.Equal(entries[0].Data, []byte("somedata")) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, []byte("somedata"))
	}
	if entries[1].EntryType != pb.EntryType_EntryConfChange {
		t.Fatalf("type = %v, want %v", entries[1].EntryType, pb.EntryType_EntryConfChange)
	}
	if !bytes.Equal(entries[1].Data, ccdata) {
		t.Errorf("data = %v, want %v", entries[1].Data, ccdata)
	}
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestRawNodeProposeAddDuplicateNode(t *testing.T) {
	s := NewMemoryStorage()
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s), []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		if rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Advance(rd)
			if idx := rd.appliedCursor(); idx > 0 {
				rawNode.AdvanceApply(idx)
			}
			break
		}
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}

	proposeConfChangeAndApply := func(cc pb.ConfChange) {
		rawNode.ProposeConfChange([]byte(""), cc)
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		for _, entry := range rd.CommittedEntries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
				var cc pb.ConfChange
				cc.Unmarshal(entry.Data)
				rawNode.ApplyConfChange(cc)
			}
		}
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}

	cc1 := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
	ccdata1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc1)

	// try to add the same node again
	proposeConfChangeAndApply(cc1)

	// the new node join should be ok
	cc2 := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 2}
	ccdata2, err := cc2.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc2)

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// the last three entries should be: ConfChange cc1, cc1, cc2
	entries, err := s.Entries(lastIndex-2, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 3)
	}
	if !bytes.Equal(entries[0].Data, ccdata1) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, ccdata1)
	}
	if !bytes.Equal(entries[2].Data, ccdata2) {
		t.Errorf("entries[2].Data = %v, want %v", entries[2].Data, ccdata2)
	}
}

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MessageType_MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
func TestRawNodeReadIndex(t *testing.T) {
	msgs := []pb.Message{}
	appendStep := func(r *Raft, m pb.Message) error {
		msgs = append(msgs, m)
		return nil
	}
	wrs := []ReadState{{Index: uint64(1), RequestCtx: []byte("somedata")}}

	s := NewMemoryStorage()
	c := newTestConfig(1, nil, 10, 1, s)
	rawNode, err := NewRawNode(c, []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rawNode.Raft.readStates = wrs
	// ensure the ReadStates can be read out
	hasReady := rawNode.HasReady()
	if !hasReady {
		t.Errorf("HasReady() returns %t, want %t", hasReady, true)
	}
	rd := rawNode.Ready()
	if !reflect.DeepEqual(rd.ReadStates, wrs) {
		t.Errorf("ReadStates = %d, want %d", rd.ReadStates, wrs)
	}
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}
	// ensure raft.readStates is reset after advance
	if rawNode.Raft.readStates != nil {
		t.Errorf("readStates = %v, want %v", rawNode.Raft.readStates, nil)
	}

	wrequestCtx := []byte("somedata2")
	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)

		if rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Advance(rd)
			if idx := rd.appliedCursor(); idx > 0 {
				rawNode.AdvanceApply(idx)
			}

			// Once we are the leader, issue a ReadIndex request
			rawNode.Raft.step = appendStep
			rawNode.ReadIndex(wrequestCtx)
			break
		}
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}
	// ensure that MessageType_MsgReadIndex message is sent to the underlying raft
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), 1)
	}
	if msgs[0].MsgType != pb.MessageType_MsgReadIndex {
		t.Errorf("msg type = %d, want %d", msgs[0].MsgType, pb.MessageType_MsgReadIndex)
	}
	if !bytes.Equal(msgs[0].Entries[0].Data, wrequestCtx) {
		t.Errorf("data = %v, want %v", msgs[0].Entries[0].Data, wrequestCtx)
	}
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.

// // Comment out it due to proto3 breaks DeepEqual
// func TestRawNodeStart(t *testing.T) {
// 	cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
// 	ccdata, err := cc.Marshal()
// 	if err != nil {
// 		t.Fatalf("unexpected marshal error: %v", err)
// 	}
// 	wants := []Ready{
// 		{
// 			HardState: pb.HardState{Term: 1, Commit: 1, Vote: 0},
// 			Entries: []pb.Entry{
// 				{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: 1, Data: ccdata},
// 			},
// 			CommittedEntries: []pb.Entry{
// 				{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: 1, Data: ccdata},
// 			},
// 			MustSync: true,
// 		},
// 		{
// 			HardState:        pb.HardState{Term: 2, Commit: 3, Vote: 1},
// 			Entries:          []pb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
// 			CommittedEntries: []pb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
// 			MustSync:         true,
// 		},
// 	}

// 	storage := NewMemoryStorage()
// 	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage), []Peer{{ID: 1}})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	rd := rawNode.Ready()
// 	t.Logf("rd %v", rd)
// 	if !reflect.DeepEqual(rd, wants[0]) {
// 		t.Fatalf("#%d: g = %+v,\n             w   %+v", 1, rd, wants[0])
// 	} else {
// 		storage.Append(rd.Entries)
// 		rawNode.Advance(rd)
// 		if idx := rd.appliedCursor(); idx > 0 {
// 			rawNode.AdvanceApply(idx)
// 		}
// 	}
// 	storage.Append(rd.Entries)
// 	rawNode.Advance(rd)
// 	if idx := rd.appliedCursor(); idx > 0 {
// 		rawNode.AdvanceApply(idx)
// 	}

// 	rawNode.Campaign()
// 	rd = rawNode.Ready()
// 	storage.Append(rd.Entries)
// 	rawNode.Advance(rd)
// 	if idx := rd.appliedCursor(); idx > 0 {
// 		rawNode.AdvanceApply(idx)
// 	}

// 	rawNode.Propose([]byte(""), []byte("foo"))
// 	if rd = rawNode.Ready(); !reflect.DeepEqual(rd, wants[1]) {
// 		t.Errorf("#%d: g = %+v,\n             w   %+v", 2, rd, wants[1])
// 	} else {
// 		storage.Append(rd.Entries)
// 		rawNode.Advance(rd)
// 		if idx := rd.appliedCursor(); idx > 0 {
// 			rawNode.AdvanceApply(idx)
// 		}
// 	}

// 	if rawNode.HasReady() {
// 		t.Errorf("unexpected Ready: %+v", rawNode.Ready())
// 	}
// }

func TestRawNodeRestart(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 1}

	want := Ready{
		HardState: emptyState,
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
		MustSync:         false,
	}

	storage := NewMemoryStorage()
	storage.SetHardState(st)
	storage.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage), nil)
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	if !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	}
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready())
	}
}

func TestRawNodeRestartFromSnapshot(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
			Index:     2,
			Term:      1,
		},
	}
	entries := []pb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 3}

	want := Ready{
		HardState: emptyState,
		// commit up to commit index in st
		CommittedEntries: entries,
		MustSync:         false,
	}

	s := NewMemoryStorage()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rd := rawNode.Ready(); !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	} else {
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.HasReady())
	}
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()

func TestRawNodeStatus(t *testing.T) {
	storage := NewMemoryStorage()
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage), []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	status := rawNode.Status()
	if status == nil {
		t.Errorf("expected status struct, got nil")
	}
}

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
// - node learns that index 11 is committed
// - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
//   exceeds maxBytes), which isn't noticed internally by Raft
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path and removes the last entry.
// - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//   its internal applied index cursor to 10 (when it should be 9)
// - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//    write.
func TestRawNodeCommitPaginationAfterRestart(t *testing.T) {
	s := &ignoreSizeHintMemStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	persistedHardState := pb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 10,
	}

	s.hardState = persistedHardState
	s.ents = make([]pb.Entry, 10)
	var size uint64
	for i := range s.ents {
		ent := pb.Entry{
			Term:      1,
			Index:     uint64(i + 1),
			EntryType: pb.EntryType_EntryNormal,
			Data:      []byte("a"),
		}

		s.ents[i] = ent
		size += uint64(ent.Size())
	}

	cfg := newTestConfig(1, []uint64{1}, 10, 1, s)
	// Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
	// not be included in the initial rd.CommittedEntries. However, our storage will ignore
	// this and *will* return it (which is how the Commit index ended up being 10 initially).
	cfg.MaxSizePerMsg = size - uint64(s.ents[len(s.ents)-1].Size()) - 1

	s.ents = append(s.ents, pb.Entry{
		Term:      1,
		Index:     uint64(11),
		EntryType: pb.EntryType_EntryNormal,
		Data:      []byte("boom"),
	})

	rawNode, err := NewRawNode(cfg, []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}

	for highestApplied := uint64(0); highestApplied != 11; {
		rd := rawNode.Ready()
		n := len(rd.CommittedEntries)
		if n == 0 {
			t.Fatalf("stopped applying entries at index %d", highestApplied)
		}
		if next := rd.CommittedEntries[0].Index; highestApplied != 0 && highestApplied+1 != next {
			t.Fatalf("attempting to apply index %d after index %d, leaving a gap", next, highestApplied)
		}
		highestApplied = rd.CommittedEntries[n-1].Index
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
		rawNode.Step(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      1,
			From:    1, // illegal, but we get away with it
			Term:    1,
			Commit:  11,
		})
	}
}

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
func TestRawNodeBoundedLogGrowthWithPartition(t *testing.T) {
	const maxEntries = 16
	data := []byte("testdata")
	testEntry := &pb.Entry{Data: data}
	maxEntrySize := uint64(maxEntries * PayloadSize(testEntry))

	s := NewMemoryStorage()
	cfg := newTestConfig(1, []uint64{1}, 10, 1, s)
	cfg.MaxUncommittedEntriesSize = maxEntrySize
	rawNode, err := NewRawNode(cfg, []Peer{{ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	// Become the leader.
	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		if rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Advance(rd)
			if idx := rd.appliedCursor(); idx > 0 {
				rawNode.AdvanceApply(idx)
			}
			break
		}
		rawNode.Advance(rd)
		if idx := rd.appliedCursor(); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}

	// Simulate a network partition while we make our proposals by never
	// committing anything. These proposals should not cause the leader's
	// log to grow indefinitely.
	for i := 0; i < 1024; i++ {
		rawNode.Propose([]byte(""), data)
	}

	// Check the size of leader's uncommitted log tail. It should not exceed the
	// MaxUncommittedEntriesSize limit.
	checkUncommitted := func(exp uint64) {
		t.Helper()
		if a := rawNode.Raft.uncommittedSize; exp != a {
			t.Fatalf("expected %d uncommitted entry bytes, found %d", exp, a)
		}
	}
	checkUncommitted(maxEntrySize)

	// Recover from the partition. The uncommitted tail of the Raft log should
	// disappear as entries are committed.
	rd = rawNode.Ready()
	if len(rd.CommittedEntries) != maxEntries {
		t.Fatalf("expected %d entries, got %d", maxEntries, len(rd.CommittedEntries))
	}
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := rd.appliedCursor(); idx > 0 {
		rawNode.AdvanceApply(idx)
	}
	checkUncommitted(0)
}

func BenchmarkStatusProgress(b *testing.B) {
	setup := func(members int) *RawNode {
		peers := make([]uint64, members)
		for i := range peers {
			peers[i] = uint64(i + 1)
		}
		cfg := newTestConfig(1, peers, 3, 1, NewMemoryStorage())
		cfg.Logger = discardLogger
		r := newRaft(cfg)
		r.becomeFollower(1, 1)
		r.becomeCandidate()
		r.becomeLeader()
		return &RawNode{Raft: r}
	}

	for _, members := range []int{1, 3, 5, 100} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			// NB: call getStatus through rn.Status because that incurs an additional
			// allocation.
			rn := setup(members)

			b.Run("Status", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.Status()
				}
			})

			b.Run("Status-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s := rn.Status()
					var n uint64
					for _, pr := range s.Progress {
						n += pr.Match
					}
					_ = n
				}
			})

			b.Run("StatusWithoutProgress", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.StatusWithoutProgress()
				}
			})

			b.Run("WithProgress", func(b *testing.B) {
				b.ReportAllocs()
				visit := func(uint64, ProgressType, Progress) {}

				for i := 0; i < b.N; i++ {
					rn.WithProgress(visit)
				}
			})
			b.Run("WithProgress-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var n uint64
					visit := func(_ uint64, _ ProgressType, pr Progress) {
						n += pr.Match
					}
					rn.WithProgress(visit)
					_ = n
				}
			})
		})
	}
}
