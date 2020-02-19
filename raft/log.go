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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog mange the log entries, its struct look like:
//
//  truntated.....first.....stabled....committed.....applied....last
//  --------|     |------------------------------------------------|
//  snapshot                          log entries
//
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

	// the incoming unstable snapshot, if any.
	pending_snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries   []pb.Entry
	offset    uint64 // truntated index + 1
	snapTerm  uint64
	snapIndex uint64
	stabled   uint64

	logger Logger
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
func newLog(storage Storage, logger Logger) *RaftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &RaftLog{
		storage: storage,
		logger:  logger,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	snapTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		panic(err)
	}
	// get all entris from storage
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	log.entries = entries
	// offset is used to manipulate entries slice, logic index - offset = slice index
	log.offset = firstIndex
	// snapIndex and snapTerm are the most recent snapshot's index and term
	log.snapIndex = firstIndex - 1
	log.snapTerm = snapTerm
	// log entries with index <= stabled are stabled to storage
	log.stabled = lastIndex
	// Initialize our committed and applied pointers to the time of the last compaction.
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, offset=%d, len(Entries)=%d", l.committed, l.applied, l.offset, len(l.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.truncateAndAppend(ents)
	l.maybeCompact()
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	if after == l.LastIndex()+1 {
		l.entries = append(l.entries, ents...)
		return
	}
	// truncate to after and copy to u.entries then append
	l.logger.Infof("truncate the unstable entries before index %d", after)
	if after-1 < l.stabled {
		l.stabled = after - 1
	}
	l.entries = append([]pb.Entry{}, l.entries[:after-l.offset]...)
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) maybeCompact() {
	fi, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	ft, err := l.storage.Term(fi - 1)
	if err != nil {
		panic(err)
	}
	compactSize := fi - l.offset
	if compactSize > 0 && compactSize < uint64(len(l.entries)) {
		l.entries = l.entries[compactSize:]
		l.offset = fi
		l.snapIndex = fi - 1
		l.snapTerm = ft
	}
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnRangeErr(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) unstableEntries() []pb.Entry {
	if int(l.stabled+1-l.offset) > len(l.entries) {
		return nil
	}
	return l.entries[l.stabled+1-l.offset:]
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.nextEntsSince(l.applied)
}

func (l *RaftLog) nextEntsSince(sinceIdx uint64) (ents []pb.Entry) {
	off := max(sinceIdx+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy RaftLog.slice() in RaftLog.nextEnts().
func (l *RaftLog) hasNextEnts() bool {
	return l.hasNextEntsSince(l.applied)
}

func (l *RaftLog) hasNextEntsSince(sinceIdx uint64) bool {
	off := max(sinceIdx+1, l.firstIndex())
	return l.committed+1 > off
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pending_snapshot != nil {
		return *l.pending_snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) firstIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[0].Index
	}
	if l.pending_snapshot != nil {
		return l.pending_snapshot.Metadata.Index + 1
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pending_snapshot != nil {
		return l.pending_snapshot.Metadata.Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) stableTo(i, t uint64) {
	if l.matchTerm(i, t) && l.stabled < i {
		l.stabled = i
	}
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pending_snapshot != nil && l.pending_snapshot.Metadata.Index == i {
		l.pending_snapshot = nil
	}
}

func (l *RaftLog) lastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.snapIndex {
		return l.snapTerm, nil
	}
	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}
	if i < l.offset {
		return 0, ErrCompacted
	}
	if i > l.offset+uint64(len(l.entries))-1 {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.offset].Term, nil
}

func (l *RaftLog) Entries(i uint64) ([]pb.Entry, error) {
	if i < l.firstIndex() {
		return nil, ErrCompacted
	}
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.entries[i-l.offset:], nil
}

// allEntries returns all entries in the log.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.LastIndex())
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	if t, err := l.Term(i); err == nil {
		return t == term
	}
	return false
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.matchTerm(maxIndex, term) {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.entries = nil
	l.stabled = s.Metadata.Index
	l.offset = s.Metadata.Index + 1
	l.snapIndex = s.Metadata.Index
	l.snapTerm = s.Metadata.Term
	l.pending_snapshot = &s
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if err := l.mustCheckOutOfBounds(lo, hi); err != nil {
		return nil, err
	}
	return l.entries[lo-l.offset : hi-l.offset], nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	if lo < fi || hi > l.LastIndex()+1 {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) zeroTermOnRangeErr(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
