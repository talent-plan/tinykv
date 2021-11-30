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
	"log"
)

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
	// 未压缩到存储的日志数量
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 还未存储的快照信息
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//unstable *unstable
	// 陈旧的数组会被清除掉，firstIndex是未被清理的最小日志index，初始化时即applied == firstIndex -1
	firstIndex uint64
	lastIndex  uint64
	// offset
	//offset     uint64
	//term   uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// get first & last Index
	if storage == nil {
		log.Panic("storage must not be nil")
	}

	firstIdx, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIdx, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// get hardState info
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	// get entries [l,r) ，并不包含首个snap_log
	ents, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		panic(err)
	}
	// return a new log`
	return &RaftLog{
		storage:         storage,
		committed:       hardState.GetCommit(),
		applied:         firstIdx - 1, // 初始化时候，entries[0].index == applyIndex
		stabled:         lastIdx,      // 初始化时候，从storage中回复的日志都是stabled状态
		entries:         ents,
		pendingSnapshot: nil,
		firstIndex:      firstIdx,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		dis := l.stabled - l.FirstIndex() + 1
		if dis < 0 || dis > uint64(len(l.entries)) {
			return nil
		}
		return l.entries[dis:]
	}
	return nil
}

//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	count := uint64(len(l.entries))
	if count > 0 {
		commitDst := l.committed - l.FirstIndex() + 1
		applyDst := l.applied - l.FirstIndex() + 1
		if commitDst < 0 || commitDst > count || applyDst < 0 || applyDst > count || commitDst < applyDst {
			return nil
		}
		// update commitDst ==> commitDst+1
		return l.entries[applyDst : commitDst+1]
	}

	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	count := uint64(len(l.entries))
	if count == 0 {
		// find on storage
		idx, err := l.storage.LastIndex()
		if err != nil {
			panic(err)
		}
		return idx
	}
	return l.entries[count-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	dummyIdx := l.firstIndex - 1
	if i < dummyIdx {
		return 0, nil
	} else if i > l.lastIndex {
		return 0, ErrInvalidIndex
	}

	// find on entries
	count := uint64(len(l.entries))
	if count > 0 {
		firstIdx := l.FirstIndex()
		if i >= firstIdx {
			indexDst := i - firstIdx
			if indexDst >= count {
				return 0, ErrUnavailable
			}
			return l.entries[indexDst].Term, nil
		}
		// i < firstIdx maybe i == dummyIdx need find on storage
	}

	// TODO:question i < firstIdx maybe i == dummyIdx need find on storage
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}

	if err == ErrCompacted || err == ErrUnavailable {
		return 0, nil
	}
	panic(err)
}

// log 中的entry并不包含快照
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		// find in storage
		// 存储中的firstIndex就是出快照之后的首份log
		idx, err := l.storage.FirstIndex()
		if err != nil {
			panic(err)
		}
		return idx
	}
	return l.entries[0].Index
}
