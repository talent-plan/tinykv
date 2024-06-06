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
	"fmt"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// snapshot, err := storage.Snapshot()
	// if err != nil {
	// 	panic(err)
	// }

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	entries = append([]pb.Entry{{
		EntryType: pb.EntryType_EntryNormal,
		// Term:      snapshot.Metadata.Term,
		// Index:     snapshot.Metadata.Index,
		Data: []byte{},
	}}, entries...)

	return &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
		// pendingSnapshot: &snapshot,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 1 {
		return nil
	}

	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	unstableStart := l.stabled + 1
	offset := l.entries[0].Index

	if int(unstableStart-offset) >= len(l.entries) {
		return []pb.Entry{}
	}

	return l.entries[unstableStart-offset : len(l.entries)]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := l.entries[0].Index

	// TODO: 想清楚 applied > committed 的情况
	if int(l.applied+1-offset) >= len(l.entries) || l.applied > l.committed {
		return nil
	}

	return l.entries[l.applied+1-offset : l.committed+1-offset]
}

func (l *RaftLog) slice(lo, hi uint64) (ents []pb.Entry) {
	if lo >= hi {
		return nil
	}

	offset := l.entries[0].Index

	if int(lo-offset) >= len(l.entries) {
		return nil
	}

	return l.entries[lo-offset : hi-offset]

}

func (l *RaftLog) FirstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		panic("no entry!")
	}

	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	offset := l.entries[0].Index
	i -= offset

	if i >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}

	return l.entries[i].Term, nil
}

func (l *RaftLog) EntryAt(index uint64) (pb.Entry, bool) {
	offset := l.entries[0].Index

	index -= offset

	if index >= uint64(len(l.entries)) {
		return pb.Entry{}, false
	}

	return l.entries[index], true
}

// TODO: 考虑全部被 truncate 的情况
func (l *RaftLog) TruncateFromIndex(index uint64) {
	offset := l.entries[0].Index

	index -= offset

	l.entries = l.entries[:index]

	l.stabled = min(l.stabled, index-1)
}

func (l *RaftLog) Append(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) String() string {
	var entries []string
	for i := 1; i < len(l.entries); i++ {
		entries = append(entries, fmt.Sprintf("{Term: %v, Index: %v}", l.entries[i].Term, l.entries[i].Index))
	}

	entriesStr := strings.Join(entries, ", ")

	return fmt.Sprintf("{applied: %v, committed: %v, stabled: %v, entries: [%v]}", l.applied, l.committed, l.stabled, entriesStr)
}
