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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

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
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rf := &Raft{
		id:                    c.ID,
		Term:                  0,
		Vote:                  0,
		RaftLog:               newLog(c.Storage),
		Prs:                   make(map[uint64]*Progress),
		State:                 StateFollower,
		votes:                 make(map[uint64]bool),
		msgs:                  make([]pb.Message, 0),
		Lead:                  c.ID,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:      0,
		electionElapsed:       0,
	}
	for _, id := range c.peers {
		rf.Prs[id] = &Progress{}
	}
	return rf
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	index := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return false
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Entries: r.RaftLog.sendEnts(r.Prs[to].Next),
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) bool {
	index := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return false
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		LogTerm: logTerm,
	})
	return true
}

func (r *Raft) sendResponse(to uint64, reject bool, msgType pb.MessageType) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: msgType,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.handleBeat()
		}
		for id, prs := range r.Prs {
			if id != r.id && prs.Next <= r.RaftLog.LastIndex() {
				r.sendAppend(id)
			}
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.handleHup()
			r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0

	if len(r.Prs) == 1 {
		r.State = StateLeader
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0

	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgBeat:
		r.handleBeat()
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	for {
		if m.Term < r.Term {
			break
		}
		r.handleHeartbeat(m)
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || logTerm != m.LogTerm {
			break
		}
		r.RaftLog.replaceTail(m.Index+1, m.Entries)
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		}
		r.sendResponse(m.From, false, pb.MessageType_MsgAppendResponse)
		return
	}
	r.sendResponse(m.From, true, pb.MessageType_MsgAppendResponse)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term || m.Term == r.Term && r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	} else if m.Term == r.Term {
		r.electionElapsed = 0
	}
	//r.sendResponse(m.From, false, pb.MessageType_MsgHeartbeatResponse)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	for {
		if m.Term < r.Term {
			break
		}
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		} else if m.Term == r.Term {
			r.electionElapsed = 0
		}
		//r.handleHeartbeat(m)
		if r.Vote != 0 && r.Vote != m.From {
			break
		}
		lastIndex := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm < logTerm {
			break
		}
		if m.LogTerm == logTerm && m.Index < lastIndex {
			break
		}
		r.Vote = m.From
		r.sendResponse(m.From, false, pb.MessageType_MsgRequestVoteResponse)
		return
	}
	r.sendResponse(m.From, true, pb.MessageType_MsgRequestVoteResponse)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	r.handleHeartbeatResponse(m)
	if !m.Reject {
		r.Prs[m.From] = &Progress{
			Match: m.Index + uint64(len(m.Entries)),
			Next:  m.Index + uint64(len(m.Entries)) + 1,
		}

		var match []uint64
		for _, prs := range r.Prs {
			match = append(match, prs.Match)
		}
		sort.Slice(match, func(i, j int) bool { return match[i] < match[j] })
		majority := match[(len(r.Prs)-1)/2]
		logTerm, _ := r.RaftLog.Term(majority)
		if majority > r.RaftLog.committed && logTerm == r.Term {
			r.RaftLog.committed = majority
		}
	} else {
		r.Prs[m.From].Next = max(1, r.Prs[m.From].Next-1)
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.handleHeartbeatResponse(m)
	r.votes[m.From] = !m.Reject

	voteCount := 0
	for _, voteGranted := range r.votes {
		if voteGranted {
			voteCount++
		}
	}

	if voteCount > len(r.Prs)/2 && r.State == StateCandidate {
		r.becomeLeader()
	} else if r.State != StateCandidate || len(r.votes) == len(r.Prs) {
		r.votes = make(map[uint64]bool)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State == StateLeader {
		r.RaftLog.replaceTail(r.RaftLog.LastIndex()+1, m.Entries)
	}
}

func (r *Raft) handleHup() {
	if r.State != StateLeader {
		r.becomeCandidate()
		for id := range r.Prs {
			if id != r.id {
				r.sendRequestVote(id)
			}
		}
	}
}

func (r *Raft) handleBeat() {
	if r.State == StateLeader {
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
	}
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
