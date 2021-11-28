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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
// raft node log
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 负责持久化数据
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// commit index
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 应用本地状态机的index
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 持久化的最后一个，初始化时候index==lastIndex
	stabled uint64

	// all entries that have not yet compact.
	// 压缩到存储的日志数量
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 还未存储的快照信息
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// 陈旧的数组会被清除掉，firstIndex是未被清理的最小日志index，即applied == firstIndex -1
	firstIndex uint64
	//lastIndex  uint64 == stabled ??
	term   uint64
	length uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// get first & last Index
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	// get hardState info
	hardState, _, _ := storage.InitialState()

	// get entries [l,r)
	ents, _ := storage.Entries(firstIndex, lastIndex+1)

	// return a new log
	return &RaftLog{
		storage:   storage,
		committed: hardState.GetCommit(),
		// TODO：why entry[0].index+1 ?
		// 日志从1开始，存储的index从0开始？
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         ents,
		pendingSnapshot: nil,
		firstIndex:      firstIndex,
		term:            hardState.GetTerm(),
		length:          uint64(len(ents)),
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
	if l.Length() > 0 {
		length := l.stabled - l.FirstIndex() + 1
		if length < 0 || length > l.Length() {
			return nil
		}
		return l.entries[length:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		commitLength := l.committed - l.FirstIndex() + 1
		applyLength := l.applied - l.FirstIndex() + 1
		if commitLength < 0 || applyLength > l.Length() {
			return nil
		}
		if applyLength >= 0 && commitLength <= l.Length() {
			return l.entries[applyLength:commitLength]
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if l.Length() == 0 {
		// find on storage
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[l.length-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l.Length() > 0 {
		currFirstIndex := l.FirstIndex()
		if i >= currFirstIndex {
			index := i - currFirstIndex
			if index >= l.Length() {
				return 0, ErrUnavailable
			} else {
				return l.entries[index].Term, nil
			}

		} else {
			return 0, ErrInvalidIndex
		}
	}

	// l.Length == 0
	// TODO:question
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

func (l *RaftLog) FirstIndex() uint64 {
	if l.Length() == 0 {
		// find in storage
		index, _ := l.storage.FirstIndex()
		return index - 1
	}
	return l.entries[0].Index
}

func (l *RaftLog) Length() uint64 {
	// sync length
	if uint64(len(l.entries)) != l.length {
		l.length = uint64(len(l.entries))
	}
	return l.length
}
