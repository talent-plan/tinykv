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

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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

	// your data here (2a).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIdx, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage: storage,
		applied: firstIdx - 1,
		committed: firstIdx - 1,
		stabled: lastIdx + 1,
		entries: make([]pb.Entry, 0),
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled > uint64(len(l.entries)) {
		return nil
	}
	return l.entries[l.stabled :]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[:l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		lastIdx, err := l.storage.LastIndex()
		if err != nil {
			panic("[LastIndex]: storage get LastIndex err")
		}
		return lastIdx
	}
	return l.stabled + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//firstIdx, err := l.firstIndex()
	//if err != nil {
	//	return 0, err
	//}
	// should validate i , not before firstIndex

	// 1. 先从 unstable 中，拿 term
	if t, ok := l.termFromUnstable(i); ok {
		return t, nil
	}

	// 2. unstable 拿不到时，去 storage 中拿
	t, err 	:= l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return t, nil
}

func (l *RaftLog) termFromUnstable(i uint64) (uint64, bool) {
	// 这个判断依据是什么
	// i < firstIdx || i > l.LastIndex
	if  i > l.LastIndex() {
		log.Errorf(fmt.Sprintf("[Term]: index:%d out of range. log entry :%d", i, len(l.entries)))
		return 0, false
	}

	if int(i) > len(l.entries) - 1 {
		return 0, false
	}

	return l.entries[i].Term, true
}

func (l *RaftLog) firstIndex() (uint64, error) {
	if len(l.entries) == 0 {
		return 0, nil
	}
	return l.entries[0].Index, nil
}