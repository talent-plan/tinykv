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
	"reflect"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/raftpb"
)

// TODO: Delete method
func TestFindConflict(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	tests := []struct {
		ents      []pb.Entry
		wconflict uint64
	}{
		// no conflict, empty ent
		{[]pb.Entry{}, 0},
		// no conflict
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 3, Term: 3}}, 0},
		// no conflict, but has new entries
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		// conflicts with existing entries
		{[]pb.Entry{{Index: 1, Term: 4}, {Index: 2, Term: 4}}, 1},
		{[]pb.Entry{{Index: 2, Term: 1}, {Index: 3, Term: 4}, {Index: 4, Term: 4}}, 2},
		{[]pb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}, {Index: 5, Term: 4}, {Index: 6, Term: 4}}, 3},
	}

	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append(previousEnts...)

		gconflict := raftLog.findConflict(tt.ents)
		if gconflict != tt.wconflict {
			t.Errorf("#%d: conflict = %d, want %d", i, gconflict, tt.wconflict)
		}
	}
}

// TODO: Delete method
func TestIsUpToDate(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	raftLog := newLog(NewMemoryStorage())
	raftLog.append(previousEnts...)
	tests := []struct {
		lastIndex uint64
		term      uint64
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.LastIndex() - 1, 4, true},
		{raftLog.LastIndex(), 4, true},
		{raftLog.LastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.LastIndex() - 1, 2, false},
		{raftLog.LastIndex(), 2, false},
		{raftLog.LastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.LastIndex() - 1, 3, false},
		{raftLog.LastIndex(), 3, true},
		{raftLog.LastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		gUpToDate := raftLog.isUpToDate(tt.lastIndex, tt.term)
		if gUpToDate != tt.wUpToDate {
			t.Errorf("#%d: uptodate = %v, want %v", i, gUpToDate, tt.wUpToDate)
		}
	}
}

// TODO: Delete method
func TestAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	tests := []struct {
		ents     []pb.Entry
		windex   uint64
		wents    []pb.Entry
		wstabled uint64
	}{
		{
			[]pb.Entry{},
			2,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			2,
		},
		{
			[]pb.Entry{{Index: 3, Term: 2}},
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}},
			2,
		},
		// conflicts with index 1
		{
			[]pb.Entry{{Index: 1, Term: 2}},
			1,
			[]pb.Entry{{Index: 1, Term: 2}},
			0,
		},
		// conflicts with index 2
		{
			[]pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}},
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}},
			1,
		},
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(previousEnts)
		raftLog := newLog(storage)

		index := raftLog.append(tt.ents...)
		if index != tt.windex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, index, tt.windex)
		}
		g, err := raftLog.Entries(1)
		if err != nil {
			t.Fatalf("#%d: unexpected error %v", i, err)
		}
		if !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: logEnts = %+v, want %+v", i, g, tt.wents)
		}
		if goff := raftLog.stabled; goff != tt.wstabled {
			t.Errorf("#%d: stabled = %d, want %d", i, goff, tt.wstabled)
		}
	}
}

// TODO: Delete method
func TestTruncateAndAppend(t *testing.T) {
	tests := []struct {
		entries  []pb.Entry
		toappend []pb.Entry

		wentries []pb.Entry
	}{
		// append to the end
		{
			[]pb.Entry{{Index: 5, Term: 1}},
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
		},
		// truncate and append
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			[]pb.Entry{{Index: 6, Term: 2}},
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 8, Term: 2}},
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 2}, {Index: 8, Term: 2}},
		},
	}

	for i, tt := range tests {
		u := RaftLog{
			entries: tt.entries,
			offset:  tt.entries[0].Index,
		}
		u.truncateAndAppend(tt.toappend)
		if !reflect.DeepEqual(u.entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, u.entries, tt.wentries)
		}
	}
}

// TODO: Delete method
// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
// 	1. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it
// 	2.Append any new entries not already in the log
// If the given (index, term) does not match with the existing log:
// 	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := uint64(3)
	lastterm := uint64(3)
	commit := uint64(1)

	tests := []struct {
		logTerm   uint64
		index     uint64
		committed uint64
		ents      []pb.Entry

		wlasti  uint64
		wappend bool
		wcommit uint64
		wpanic  bool
	}{
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, []pb.Entry{{Index: lastindex + 2, Term: 4}},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, nil,
			lastindex, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, nil,
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex - 1, nil,
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			lastterm, lastindex, 0, nil,
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			0, 0, lastindex, nil,
			0, true, commit, false, // commit do not decrease
		},
		{
			lastterm, lastindex, lastindex, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			lastterm, lastindex, lastindex + 2, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex + 2, []pb.Entry{{Index: lastindex + 1, Term: 4}, {Index: lastindex + 2, Term: 4}},
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the the entry in the middle
		{
			lastterm - 1, lastindex - 1, lastindex, []pb.Entry{{Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []pb.Entry{{Index: lastindex - 1, Term: 4}},
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			lastterm - 3, lastindex - 3, lastindex, []pb.Entry{{Index: lastindex - 2, Term: 4}},
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []pb.Entry{{Index: lastindex - 1, Term: 4}, {Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append(previousEnts...)
		raftLog.committed = commit
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			glasti, gappend := raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents...)
			gcommit := raftLog.committed

			if glasti != tt.wlasti {
				t.Errorf("#%d: lastindex = %d, want %d", i, glasti, tt.wlasti)
			}
			if gappend != tt.wappend {
				t.Errorf("#%d: append = %v, want %v", i, gappend, tt.wappend)
			}
			if gcommit != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, gcommit, tt.wcommit)
			}
			if gappend && len(tt.ents) != 0 {
				gents, err := raftLog.slice(raftLog.LastIndex()-uint64(len(tt.ents))+1, raftLog.LastIndex()+1)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tt.ents, gents) {
					t.Errorf("%d: appended entries = %v, want %v", i, gents, tt.ents)
				}
			}
		}()
	}
}

// TODO: Delete method
// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	var i uint64
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	lastIndex := uint64(1000)
	unstableIndex := uint64(750)
	lastTerm := lastIndex
	storage := NewMemoryStorage()
	for i = 1; i <= unstableIndex; i++ {
		storage.Append([]pb.Entry{{Term: i, Index: i}})
	}
	raftLog := newLog(storage)
	for i = unstableIndex; i < lastIndex; i++ {
		raftLog.append(pb.Entry{Term: i + 1, Index: i + 1})
	}

	ok := raftLog.maybeCommit(lastIndex, lastTerm)
	if !ok {
		t.Fatalf("maybeCommit returned false")
	}
	raftLog.appliedTo(raftLog.committed)

	offset := uint64(500)
	storage.Compact(offset)
	raftLog.maybeCompact()

	if raftLog.LastIndex() != lastIndex {
		t.Errorf("lastIndex = %d, want %d", raftLog.LastIndex(), lastIndex)
	}

	for j := offset; j <= raftLog.LastIndex(); j++ {
		if mustTerm(raftLog.Term(j)) != j {
			t.Errorf("term(%d) = %d, want %d", j, mustTerm(raftLog.Term(j)), j)
		}
	}

	for j := offset; j <= raftLog.LastIndex(); j++ {
		if !raftLog.matchTerm(j, j) {
			t.Errorf("matchTerm(%d) = false, want true", j)
		}
	}

	unstableEnts := raftLog.unstableEntries()
	if g := len(unstableEnts); g != 250 {
		t.Errorf("len(unstableEntries) = %d, want = %d", g, 250)
	}
	if unstableEnts[0].Index != 751 {
		t.Errorf("Index = %d, want = %d", unstableEnts[0].Index, 751)
	}

	prev := raftLog.LastIndex()
	raftLog.append(pb.Entry{Index: raftLog.LastIndex() + 1, Term: raftLog.LastIndex() + 1})
	if raftLog.LastIndex() != prev+1 {
		t.Errorf("lastIndex = %d, want = %d", raftLog.LastIndex(), prev+1)
	}

	ents, err := raftLog.Entries(raftLog.LastIndex())
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(ents) != 1 {
		t.Errorf("len(entries) = %d, want = %d", len(ents), 1)
	}
}

// TODO: Delete method
func TestHasNextEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		hasNext bool
	}{
		{0, true},
		{3, true},
		{4, true},
		{5, false},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.ApplySnapshot(snap)
		raftLog := newLog(storage)
		raftLog.append(ents...)
		raftLog.maybeCommit(5, 1)
		raftLog.appliedTo(tt.applied)

		hasNext := raftLog.hasNextEnts()
		if hasNext != tt.hasNext {
			t.Errorf("#%d: hasNext = %v, want %v", i, hasNext, tt.hasNext)
		}
	}
}

// TODO: Delete method
func TestNextEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		wents   []pb.Entry
	}{
		{0, ents[:2]},
		{3, ents[:2]},
		{4, ents[1:2]},
		{5, nil},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.ApplySnapshot(snap)
		raftLog := newLog(storage)
		raftLog.append(ents...)
		raftLog.maybeCommit(5, 1)
		raftLog.appliedTo(tt.applied)

		nents := raftLog.nextEnts()
		if !reflect.DeepEqual(nents, tt.wents) {
			t.Errorf("#%d: nents = %+v, want %+v", i, nents, tt.wents)
		}
	}
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestUnstableEnts(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}, {Term: 4, Index: 4}}
	emptyEnt := []pb.Entry{}
	tests := []struct {
		initstable uint64
		wents      []pb.Entry
	}{
		{0, previousEnts},
		{1, previousEnts[1:]},
		{2, previousEnts[2:]},
		{3, previousEnts[3:]},
		{4, emptyEnt},
	}

	for i, tt := range tests {
		// append stable entries to storage
		storage := NewMemoryStorage()
		storage.Append(previousEnts[:tt.initstable])
		// append unstable entries to raftlog
		raftLog := newLog(storage)
		raftLog.entries = append(raftLog.entries, previousEnts[tt.initstable:]...)

		ents := raftLog.unstableEntries()
		if !reflect.DeepEqual(ents, tt.wents) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, ents, tt.wents)
		}
	}
}

// TODO: Delete method
func TestCommitTo(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}
	commit := uint64(2)
	tests := []struct {
		commit  uint64
		wcommit uint64
		wpanic  bool
	}{
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	}
	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			raftLog := newLog(NewMemoryStorage())
			raftLog.append(previousEnts...)
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			if raftLog.committed != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, raftLog.committed, tt.wcommit)
			}
		}()
	}
}

// TODO: Delete method
func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei  uint64
		stablet  uint64
		wstabled uint64
	}{
		{1, 1, 1},
		{2, 2, 2},
		{2, 1, 0}, // bad term
		{3, 1, 0}, // bad index
	}
	for i, tt := range tests {
		raftLog := newLog(NewMemoryStorage())
		raftLog.append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}...)
		raftLog.stableTo(tt.stablei, tt.stablet)
		if raftLog.stabled != tt.wstabled {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.stabled, tt.wstabled)
		}
	}
}

// TODO: Delete method
func TestStableToWithSnap(t *testing.T) {
	snapi, snapt := uint64(5), uint64(2)
	tests := []struct {
		stablei uint64
		stablet uint64
		newEnts []pb.Entry

		wstabled uint64
	}{
		{snapi + 1, snapt, nil, snapi},
		{snapi, snapt, nil, snapi},
		{snapi - 1, snapt, nil, snapi},

		{snapi + 1, snapt + 1, nil, snapi},
		{snapi, snapt + 1, nil, snapi},
		{snapi - 1, snapt + 1, nil, snapi},

		{snapi + 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi},
		{snapi - 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi},

		{snapi + 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi},
		{snapi, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi},
		{snapi - 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi},
	}
	for i, tt := range tests {
		s := NewMemoryStorage()
		s.ApplySnapshot(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: snapi, Term: snapt}})
		raftLog := newLog(s)
		raftLog.append(tt.newEnts...)
		raftLog.stableTo(tt.stablei, tt.stablet)
		if raftLog.stabled != tt.wstabled {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.stabled, tt.wstabled)
		}
	}
}

//TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	lastIndex := uint64(1000)
	compact := []uint64{300, 500, 800, 900}
	wleft := []int{700, 500, 200, 100}

	storage := NewMemoryStorage()
	for i := uint64(1); i <= lastIndex; i++ {
		storage.Append([]pb.Entry{{Index: i}})
	}
	raftLog := newLog(storage)
	raftLog.committed = lastIndex
	raftLog.applied = lastIndex

	for j := 0; j < len(compact); j++ {
		storage.Compact(compact[j])
		raftLog.maybeCompact()
		if len(raftLog.allEntries()) != wleft[j] {
			t.Errorf("len = %d, want %d", len(raftLog.allEntries()), wleft[j])
		}
	}
}

// TODO: Delete method
func TestLogRestore(t *testing.T) {
	index := uint64(1000)
	term := uint64(1000)
	snap := &pb.SnapshotMetadata{Index: index, Term: term}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: snap})
	raftLog := newLog(storage)

	l, err := raftLog.slice(raftLog.firstIndex(), raftLog.LastIndex()+1)
	if err != nil {
		t.Errorf("unexpect err %d", err)
	}
	if len(l) != 0 {
		t.Errorf("len = %d, want 0", len(l))
	}
	if len(raftLog.unstableEntries()) != 0 {
		t.Errorf("len = %d, want 0", len(raftLog.unstableEntries()))
	}
	if len(raftLog.nextEnts()) != 0 {
		t.Errorf("len = %d, want 0", len(raftLog.nextEnts()))
	}
	if len(raftLog.allEntries()) != 0 {
		t.Errorf("len = %d, want 0", len(raftLog.allEntries()))
	}
	if raftLog.firstIndex() != index {
		t.Errorf("firstIndex = %d, want %d", raftLog.firstIndex(), index+1)
	}
	if raftLog.committed != index {
		t.Errorf("committed = %d, want %d", raftLog.committed, index)
	}
	if raftLog.offset != index+1 {
		t.Errorf("unstable = %d, want %d", raftLog.offset, index+1)
	}
	if mustTerm(raftLog.Term(index)) != term {
		t.Errorf("term = %d, want %d", mustTerm(raftLog.Term(index)), term)
	}
}

// TODO: Delete method
func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: offset}})
	l := newLog(storage)
	for i := uint64(1); i <= num; i++ {
		l.append(pb.Entry{Index: i + offset})
	}

	first := offset + 1
	tests := []struct {
		lo, hi        uint64
		wpanic        bool
		wErrCompacted bool
	}{
		{
			first - 2, first + 1,
			false,
			true,
		},
		{
			first - 1, first + 1,
			false,
			true,
		},
		{
			first, first,
			false,
			false,
		},
		{
			first + num/2, first + num/2,
			false,
			false,
		},
		{
			first + num - 1, first + num - 1,
			false,
			false,
		},
		{
			first + num, first + num,
			false,
			false,
		},
		{
			first + num, first + num + 1,
			true,
			false,
		},
		{
			first + num + 1, first + num + 1,
			true,
			false,
		},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", i, true, false, r)
					}
				}
			}()
			err := l.mustCheckOutOfBounds(tt.lo, tt.hi)
			if tt.wpanic {
				t.Errorf("%d: panic = %v, want %v", i, false, true)
			}
			if tt.wErrCompacted && err != ErrCompacted {
				t.Errorf("%d: err = %v, want %v", i, err, ErrCompacted)
			}
			if !tt.wErrCompacted && err != nil {
				t.Errorf("%d: unexpected err %v", i, err)
			}
		}()
	}
}

func TestTerm(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: offset, Term: 1}})
	l := newLog(storage)
	for i = 1; i < num; i++ {
		l.entries = append(l.entries, pb.Entry{Index: offset + i, Term: i})
	}

	tests := []struct {
		index uint64
		w     uint64
		werr  bool
	}{
		{offset, 1, false},
		{offset - 1, 0, true},
		{offset, 1, false},
		{offset + num - 1, num - 1, false},
		{offset + num, 0, true},
	}

	for j, tt := range tests {
		term, err := l.Term(tt.index)
		if tt.werr {
			if err == nil {
				t.Errorf("#%d: want err but got nil", j)
			}
			continue
		}
		if term != tt.w {
			t.Errorf("#%d: at = %d, want %d", j, term, tt.w)
		}
	}
}

// TODO: Delete method
func TestTermWithUnstableSnapshot(t *testing.T) {
	storagesnapi := uint64(100)
	unstablesnapi := storagesnapi + 5

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: storagesnapi, Term: 1}})
	l := newLog(storage)
	l.restore(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: unstablesnapi, Term: 1}})

	tests := []struct {
		index uint64
		w     uint64
		werr  error
	}{
		// cannot get term from storage
		{storagesnapi, 0, ErrCompacted},
		// cannot get term from the gap between storage ents and unstable snapshot
		{storagesnapi + 1, 0, ErrCompacted},
		{unstablesnapi - 1, 0, ErrCompacted},
		{unstablesnapi + 1, 0, ErrCompacted},
		// get term from unstable snapshot index
		{unstablesnapi, 1, nil},
	}

	for i, tt := range tests {
		term, err := l.Term(tt.index)
		if term != tt.w {
			t.Errorf("#%d: at = %d, want %d", i, term, tt.w)
		}
		if err != tt.werr {
			t.Errorf("#%d: err = %d, want %d", i, err, tt.werr)
		}
	}
}

// TODO: Delete method
func TestSlice(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	last := offset + num
	half := offset + num/2

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: offset}})
	for i = 1; i < num/2; i++ {
		storage.Append([]pb.Entry{{Index: offset + i, Term: offset + i}})
	}
	l := newLog(storage)
	for i = num / 2; i < num; i++ {
		l.append(pb.Entry{Index: offset + i, Term: offset + i})
	}

	tests := []struct {
		from uint64
		to   uint64

		w      []pb.Entry
		wpanic bool
	}{
		{offset - 1, offset + 1, nil, false},
		{offset, offset + 1, nil, false},
		{half - 1, half + 1, []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half, half + 1, []pb.Entry{{Index: half, Term: half}}, false},
		{last - 1, last, []pb.Entry{{Index: last - 1, Term: last - 1}}, false},
		{last, last + 1, nil, true},
	}

	for j, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", j, true, false, r)
					}
				}
			}()
			g, err := l.slice(tt.from, tt.to)
			if tt.from <= offset && err != ErrCompacted {
				t.Fatalf("#%d: err = %v, want %v", j, err, ErrCompacted)
			}
			if tt.from > offset && err != nil {
				t.Fatalf("#%d: unexpected error %v", j, err)
			}
			if !reflect.DeepEqual(g, tt.w) {
				t.Errorf("#%d: from %d to %d = %v, want %v", j, tt.from, tt.to, g, tt.w)
			}
		}()
	}
}
