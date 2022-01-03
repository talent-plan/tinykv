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
//  offset                    log entries
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
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func (l *RaftLog) String() string{
	sFirst ,_ :=l.storage.FirstIndex()
	sLast,_:=l.storage.LastIndex()
	return fmt.Sprintf("first(%d),applied(%d),committed(%d),stabled(%d),last(%d)\n"+
								"storage.first(%d),stroage.last(%d),len of ents(%d)",
								l.first,l.applied,l.committed,l.stabled,l.LastIndex(),
								sFirst,sLast,len(l.entries))

}
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex,err := storage.FirstIndex()
	if err != nil{
		log.Panicf("firstIndex(%d) err(%+v)",firstIndex,err)
	}
	lastIndex,err := storage.LastIndex()
	if err != nil{
		log.Panicf("lastIndex(%d) err(%+v)",lastIndex,err)
	}
	entries ,err := storage.Entries(firstIndex,lastIndex+1)
	if err != nil{
		log.Panicf("storage.Entries(%d,%d),err(+%v)",firstIndex,lastIndex+1,err)
	}
	//snapshot/first.....applied....committed....stabled.....last
	raftLog :=&RaftLog{
		storage: storage,
		entries: entries,
		first: firstIndex,
		applied: firstIndex-1,
		committed: firstIndex-1,
		stabled: lastIndex,
	}
	return  raftLog
}
func (l *RaftLog) FirstIndex() uint64{
	return l.first
}
// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0{
		return l.entries[len(l.entries)-1].Index
	}
	le ,_ := l.storage.LastIndex()
	return le
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
	//  snapshot/first.....applied....committed....stabled.............last
	//  --------|------------------------------------------stabled+1----|
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.first+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
// return all the availablee entries for execution
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// Invariant: applied <= committed
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.first+1:l.committed-l.first+1]
	}
	return nil
}


// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex(){
		log.Errorf("Term i(%d) lastIndex(%d)\n",i,l.LastIndex())
		return 0 ,ErrUnavailable
	}
	if len(l.entries) > 0 && i >= l.first{
		return l.entries[i-l.first].Term , nil
	}
	term ,err := l.storage.Term(i)
	if err == ErrUnavailable && IsEmptySnap(l.pendingSnapshot){
		// find it in l.pendingSnapshot
		if i == l.pendingSnapshot.Metadata.Index{
			term  = l.pendingSnapshot.Metadata.Term
			err = nil
		}
		return term, err
	}
	return term ,err
}

func (l *RaftLog) toSliceIndex(logIndex uint64) uint64{
	//if logIndex < l.first || logIndex > l.LastIndex(){
	//	log.Panicf("toSliceIndex out of range logIndex(%d)",logIndex)
	//}
	return logIndex - l.first
}

func (l *RaftLog) toLogIndex(sliceIndex uint64) uint64{
	return sliceIndex + l.first
}

// sliceFrom return the entries slice in [i,last]
func (l *RaftLog) sliceFrom(i uint64)  []pb.Entry{
	if i>l.LastIndex()|| i<l.first || len(l.entries) == 0{
		return make([]pb.Entry,0)
	}
	return l.entries[i-l.first:]
}

func (l *RaftLog) simpleAppendEntry(term uint64,data []byte){
	entry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term: term,
		Index: l.LastIndex()+1,
		Data:data,
	}
	l.entries = append(l.entries,entry)
}