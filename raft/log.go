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

	// Your Data Here (2A).
	offset uint64
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
	entitis, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	log := &RaftLog{
		storage: storage,
		// 初始化committed和applied指针指向最后压缩的位置，当压缩之后就不能直接使用数组index将不适用
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entitis,
		offset:    firstIndex,
	}

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) append(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) GetItemByIndex(index uint64) *pb.Entry {
	return &l.entries[index-l.offset]
}

func (l *RaftLog) DeletItemByIndex(index uint64)  {
	//删掉index后的日志
	if index < l.LastIndex(){
		l.entries = l.entries[:index-l.offset+1]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// offset......stable...........end
		//                   | unstable|
		//(stable, end]
		return l.entries[l.stabled-l.offset:]
	}

	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).

	if len(l.entries) > 0 {
		// offset......applied......committed
		// (applied, committed]
		return l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
	}

	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	if i := len(l.entries); i > 0 {
		// offset......end
		// |    len     | = [offset, end]  offset = [start......offset]
		// offset + len = offset + len(include index of offset)
		// index = offset + len - 1
		return l.offset + uint64(i) - 1
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.offset <= i {
		// maybe panic, if i invalid
		return l.entries[i-l.offset].Term, nil
	}
	if l.pendingSnapshot != nil {
		if l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		} else {
			return 0, ErrCompacted
		}
	}
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (l *RaftLog) getNewCommitIndex(a uint64 ,b uint64) uint64{
	min := uint64(0)
	if a>b{
		min = b
	}else{
		min = a
	}
	return min
}
