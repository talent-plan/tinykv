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
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).

	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	raft := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	if hs.Commit < raft.RaftLog.committed || hs.Commit > raft.RaftLog.LastIndex() {
		panic("invalid hardstate commitIndex")
	}
	raft.RaftLog.committed = hs.Commit

	for _, peer := range c.peers {
		raft.Prs[uint64(peer)] = &Progress{
			Match: 0,
			Next:  0,
		}
	}

	raft.becomeFollower(raft.Term, None)

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	entries := r.RaftLog.slice(prevLogIndex+1, r.RaftLog.LastIndex()+1)

	entriesPointerSlice := []*pb.Entry{}
	for _, entry := range entries {
		entriesPointerSlice = append(entriesPointerSlice, &pb.Entry{
			EntryType:            entry.EntryType,
			Term:                 entry.Term,
			Index:                entry.Index,
			Data:                 entry.Data,
			XXX_NoUnkeyedLiteral: entry.XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     entry.XXX_unrecognized,
			XXX_sizecache:        entry.XXX_sizecache,
		})
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entriesPointerSlice,
		Commit:  r.RaftLog.committed,
	}

	r.send(msg)

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commitIndex := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  commitIndex,
	}

	r.send(msg)
}

func (r *Raft) appendEntries(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()

	for i := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType:            entries[i].EntryType,
			Term:                 r.Term,
			Index:                lastIndex + uint64(i) + 1,
			Data:                 entries[i].Data,
			XXX_NoUnkeyedLiteral: entries[i].XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     entries[i].XXX_unrecognized,
			XXX_sizecache:        entries[i].XXX_sizecache,
		})
	}

	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.electionElapsed >= r.randomElectionTimeout && r.State != StateLeader {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
	}

	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendHeartbeat(id)
	}
}

func (r *Raft) bcastAppendEntries() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendAppend(id)
	}
	r.tryUpdateCommitIndex(false)
}

func (r *Raft) startLeaderElection() {
	r.becomeCandidate()

	voteCnt := 0
	for _, vote := range r.votes {
		if vote {
			voteCnt++
		}
	}

	if voteCnt > len(r.Prs)/2 {
		r.becomeLeader()
		return
	}

	entry, exist := r.RaftLog.EntryAt(r.RaftLog.LastIndex())
	if !exist {
		panic("no entry")
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			LogTerm: entry.Term,
			Index:   entry.Index,
		})
	}
}

func (r *Raft) resetVoteRecord() {
	r.Vote = None
	r.votes = make(map[uint64]bool)
}

func (r *Raft) resetLogicClock() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term != term {
		r.resetVoteRecord()
	}
	r.Term = term
	r.Lead = lead

	r.resetLogicClock()
	r.resetRandomElectionTimeout()
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++

	// vote for self
	r.resetVoteRecord()
	r.Vote = r.id
	r.votes[r.id] = true

	r.resetLogicClock()
	r.resetRandomElectionTimeout()

	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.State = StateLeader
	r.Lead = r.id

	r.resetVoteRecord()
	r.resetLogicClock()
	r.resetRandomElectionTimeout()

	for _, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

	// no-op
	r.RaftLog.Append(pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.bcastAppendEntries()
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, r.Lead)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		// reply false if term < currentTerm
		if m.Term < r.Term {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				Reject:  true,
			})

			return nil
		}

		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		lastEntry, exist := r.RaftLog.EntryAt(r.RaftLog.LastIndex())
		if !exist {
			panic("no entry")
		}

		if (r.Vote == None || r.Vote == m.From) && (m.LogTerm > lastEntry.Term || (m.LogTerm == lastEntry.Term && m.Index >= lastEntry.Index)) {
			r.Vote = m.From
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				Reject:  false,
			})

			return nil
		}

		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Reject:  true,
		})

		return nil
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()

	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries...)
		r.bcastAppendEntries()

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResp(m)

	case pb.MessageType_MsgHeartbeatResponse:
		if r.Prs[m.From].Match < r.RaftLog.committed {
			r.sendAppend(m.From)
		}
	}

	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startLeaderElection()

	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From
		r.handleHeartbeat(m)
	default:
	}

	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startLeaderElection()

	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject

		voteCnt, rejectCnt := 0, 0
		for _, vote := range r.votes {
			if vote {
				voteCnt++
			} else {
				rejectCnt++
			}
		}

		if voteCnt > len(r.Prs)/2 {
			r.becomeLeader()
		}

		if rejectCnt > len(r.Prs)/2 {
			r.becomeFollower(r.Term, None)
		}

	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		}
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	if entry, exist := r.RaftLog.EntryAt(m.Index); !exist || entry.Term != m.LogTerm {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	firstDiff := m.Index + 1
	startCopy := 0
	for startCopy < len(m.Entries) {
		entry, exist := r.RaftLog.EntryAt(firstDiff)
		if !exist || entry.Term != m.Entries[startCopy].Term {
			r.RaftLog.TruncateFromIndex(firstDiff)
			break
		}

		firstDiff++
		startCopy++
	}

	if startCopy >= len(m.Entries) {
		r.RaftLog.committed = min(m.Commit, firstDiff-1)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.LastIndex(),
			Reject:  false,
		})
		return
	}

	r.RaftLog.TruncateFromIndex(firstDiff)

	entriesPointerSlice := []pb.Entry{}
	for _, entry := range m.Entries[startCopy:] {
		entriesPointerSlice = append(entriesPointerSlice, pb.Entry{
			EntryType:            entry.EntryType,
			Term:                 entry.Term,
			Index:                entry.Index,
			Data:                 entry.Data,
			XXX_NoUnkeyedLiteral: entry.XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     entry.XXX_unrecognized,
			XXX_sizecache:        entry.XXX_sizecache,
		})
	}
	r.RaftLog.Append(entriesPointerSlice...)

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	})
}

func (r *Raft) tryUpdateCommitIndex(bcastAppendAfterUpdateCommit bool) {
	// update committed index
	matchIndexs := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matchIndexs = append(matchIndexs, pr.Match)
	}

	sort.Slice(matchIndexs, func(i, j int) bool {
		return matchIndexs[i] < matchIndexs[j]
	})

	N := matchIndexs[(len(matchIndexs)-1)/2]
	if entry, exist := r.RaftLog.EntryAt(N); exist && N > r.RaftLog.committed && r.Term == entry.Term {
		r.RaftLog.committed = N
		if bcastAppendAfterUpdateCommit {
			r.bcastAppendEntries()
		}
	}
}

func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	if m.Term < r.Term {
		return
	}

	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)

	// update committed index
	r.tryUpdateCommitIndex(true)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.committed = max(r.RaftLog.committed, m.Commit)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
