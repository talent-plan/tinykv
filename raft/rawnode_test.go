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
	"reflect"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type ignoreSizeHintMemStorage struct {
	*MemoryStorage
}

func (s *ignoreSizeHintMemStorage) Entries(lo, hi uint64, maxSize uint64) ([]pb.Entry, error) {
	return s.MemoryStorage.Entries(lo, hi)
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func appliedCursor(rd *Ready) uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if !IsEmptySnap(&rd.Snapshot) {
		if index := rd.Snapshot.Metadata.Index; index > 0 {
			return index
		}
	}
	return 0
}

// TestRawNodeStep ensures that RawNode.Step ignore local message.
func TestRawNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		s := NewMemoryStorage()
		rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
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
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := appliedCursor(&rd); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	if d := rawNode.Ready(); !IsEmptyHardState(d.HardState) || len(d.Entries) > 0 {
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
		if idx := appliedCursor(&rd); idx > 0 {
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

	entries, err := s.Entries(lastIndex-1, lastIndex+1)
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
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := appliedCursor(&rd); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		if rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Advance(rd)
			if idx := appliedCursor(&rd); idx > 0 {
				rawNode.AdvanceApply(idx)
			}
			break
		}
		rawNode.Advance(rd)
		if idx := appliedCursor(&rd); idx > 0 {
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
		if idx := appliedCursor(&rd); idx > 0 {
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
	entries, err := s.Entries(lastIndex-2, lastIndex+1)
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

// TestRawNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
func TestRawNodeStart(t *testing.T) {
	cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
	ccdata, err := cc.Marshal()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	wants := []Ready{
		{
			HardState: pb.HardState{Term: 1, Commit: 1, Vote: 0},
			Entries: []pb.Entry{
				{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
			CommittedEntries: []pb.Entry{
				{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
		},
		{
			HardState:        pb.HardState{Term: 2, Commit: 3, Vote: 1},
			Entries:          []pb.Entry{{Term: 2, Index: 3, Context: []byte(""), Data: []byte("foo")}},
			CommittedEntries: []pb.Entry{{Term: 2, Index: 3, Context: []byte(""), Data: []byte("foo")}},
		},
	}

	storage := NewMemoryStorage()
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	t.Logf("rd %v", rd)
	if !reflect.DeepEqual(rd, wants[0]) {
		t.Fatalf("#%d: g = %+v,\n             w   %+v", 1, rd, wants[0])
	} else {
		storage.Append(rd.Entries)
		rawNode.Advance(rd)
		if idx := appliedCursor(&rd); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}
	storage.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := appliedCursor(&rd); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	rawNode.Campaign()
	rd = rawNode.Ready()
	storage.Append(rd.Entries)
	rawNode.Advance(rd)
	if idx := appliedCursor(&rd); idx > 0 {
		rawNode.AdvanceApply(idx)
	}

	rawNode.Propose([]byte(""), []byte("foo"))
	if rd = rawNode.Ready(); !reflect.DeepEqual(rd, wants[1]) {
		t.Errorf("#%d: g = %+v,\n             w   %+v", 2, rd, wants[1])
	} else {
		storage.Append(rd.Entries)
		rawNode.Advance(rd)
		if idx := appliedCursor(&rd); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}

	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready())
	}
}

func TestRawNodeRestart(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 1}

	want := Ready{
		Entries: []pb.Entry{},
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
	}

	storage := NewMemoryStorage()
	storage.SetHardState(st)
	storage.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	if !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	}
	rawNode.Advance(rd)
	if idx := appliedCursor(&rd); idx > 0 {
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
		Entries: []pb.Entry{},
		// commit up to commit index in st
		CommittedEntries: entries,
	}

	s := NewMemoryStorage()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	if rd := rawNode.Ready(); !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %+v,\n             w   %+v", rd, want)
	} else {
		rawNode.Advance(rd)
		if idx := appliedCursor(&rd); idx > 0 {
			rawNode.AdvanceApply(idx)
		}
	}
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.HasReady())
	}
}
