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

	randomizeElectionTimeout int

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
	raftLog := newLog(c.Storage)
	hs, cs, _ := raftLog.storage.InitialState()
	raftLog.committed = hs.GetCommit()
	raft := &Raft{
		id: c.ID,
		Vote: hs.GetVote(),
		RaftLog: raftLog,
		Prs: make(map[uint64]*Progress),
		votes: make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
	}
	if c.peers == nil {
		c.peers = cs.GetNodes()
	}

	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer != raft.id {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			raft.Prs[peer] = &Progress{Match: lastIndex}
		}		
	}
	
	raft.becomeFollower(hs.Term, None)
	
	return raft
}

func (r *Raft) resetElectionTimeout() {
	r.randomizeElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastPrevIndex := r.Prs[to].Next - 1
	lastIndex := r.RaftLog.LastIndex()
	lastPrevTerm, _ := r.RaftLog.Term(lastIndex)
	entries := []*pb.Entry{}
	length := len(r.RaftLog.entries)
	for i := r.RaftLog.ToRealIndex(lastPrevIndex + 1); i < uint64(length); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	msg := pb.Message {
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		Entries: entries,
		Index: lastPrevIndex,
		LogTerm: lastPrevTerm,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, success bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To: to,
		From: r.id,
		Reject: !success,
		Term: r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
	}	
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, success bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To: to,
		From: r.id,
		Reject: !success,
		Term: r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: lastTerm,
		Index: lastIndex,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, voteGranted bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: to,
		From: r.id,
		Reject: !voteGranted,
		Term: r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizeElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Vote = None
	r.Term = term
	r.Lead = lead
	r.resetElectionTimeout()
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.Term++
	r.votes[r.id] = true
	r.Lead = None
	r.resetElectionTimeout()
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = r.heartbeatTimeout
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer] = &Progress{Match: lastIndex + 1, Next: lastIndex + 2}
		} else {
			r.Prs[peer] = &Progress{Match: lastIndex, Next: lastIndex + 1}			
		}
	}
	// TODO: boardcast append no-op entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term: r.Term,
		Index: lastIndex + 1,
		EntryType: pb.EntryType_EntryNormal,
	})
	r.boardcastAppend()

}

func (r *Raft) startElection() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.boardcastRequestVote()
}

func (r *Raft) boardcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) boardcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) boardcastRequestVote() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.boardcastHeartbeat()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}
}
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, false)
		return
	}
	r.becomeFollower(m.Term, m.From)

	if !r.isLogMatch(m.Index, m.LogTerm) {
		r.sendAppendResponse(m.From, false)
		return
	}
	// append leader's entries
	lastIndex := r.RaftLog.LastIndex()
	first := 1
	for i, entry := range m.Entries {
		if entry.Index <= lastIndex {
			term, _ := r.RaftLog.Term(entry.Index)
			if term != entry.Term {
				idx := r.RaftLog.ToRealIndex(entry.Index)
				r.RaftLog.entries = r.RaftLog.entries[:idx]
				first = i
				break
			}
		} 
	}
	
	for i := first; i < len(m.Entries); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(r.RaftLog.committed, m.Index + uint64(len(m.Entries)))
	}

	r.sendAppendResponse(m.From, true)	
}

func (r *Raft) isLogMatch(prevIndex, prevTerm uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	if prevIndex > lastIndex {
		return false
	}

	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if lastTerm != prevTerm {
		return false
	}

	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, false)
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From, true)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, false)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, false)
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if m.LogTerm > lastTerm || m.LogTerm == lastTerm && m.Index >= lastIndex  {
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Reject {
		return
	}

	r.votes[m.From] = true
	voteCount := 0
	totalPeers := len(r.Prs)
	for _, vote := range r.votes {
		if vote {
			voteCount++
		}

		if voteCount > totalPeers / 2 {
			r.becomeLeader()
			return
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
