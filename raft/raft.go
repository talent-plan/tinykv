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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
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
	// randomizedElectionTimeout is a random number between
	// [ electionTimeout, 2 * electionTimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
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

	raftLog := newLog(c.Storage)
	hs, _, err := raftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:                        c.ID,
		Term:                      hs.GetTerm(),
		Vote:                      hs.GetVote(),
		RaftLog:                   raftLog,
		Prs:                       make(map[uint64]*Progress),
		State:                     StateFollower,
		Lead:                      None,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		votes:                     make(map[uint64]bool),
	}

	// set Match and Next
	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		panic(err)
	}
	for _, i := range c.peers {
		r.Prs[i] = &Progress{Next: lastIndex + 1}
	}
	r.Prs[r.id].Match = lastIndex
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	case StateCandidate, StateFollower:
		r.tickElection()
	}
}

//tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Debugf("error occurred during election: %v", err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}

	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
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
	for i := range r.Prs {
		r.Prs[i] = &Progress{
			Next: r.RaftLog.LastIndex(),
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//todo: switch func aim to pass raft_test.go/TestLeaderElection2AA
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		switch {
		case m.MsgType == pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
		case m.MsgType == pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
	case m.MsgType == pb.MessageType_MsgHeartbeat:
		r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
	}

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

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		//#todo equal is aim to pass raft_paper_test.go/TestCandidateFallback2AA
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgAppend:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	}
	return nil
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendHeartbeat(to)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}

	r.send(msg)
}

// campaign
func (r *Raft) campaign() {
	if r.State == StateLeader {
		return
	}

	r.becomeCandidate()

	// only itself alive in the cluster
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	for to := range r.Prs {
		if to == r.id {
			continue
		}
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
		if err != nil {
			panic(err)
		}
		r.sendRequestVote(to, lastLogTerm, lastLogIndex)
	}
}

// sendRequestVote
func (r *Raft) sendRequestVote(to, lastLogTerm, lastLogIndex uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	}
	r.send(msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

//rejectVote return reject this leader election or not
// true: reject;
// false: support
func (r *Raft) rejectVote(m pb.Message) bool {
	var reject bool = true

	// older term's elecetion, reject its election
	if r.Term > m.Term {
		return reject
	}

	//first come first vote, already vote for other peer
	if r.Vote != None && r.Vote != m.From {
		return reject
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}

	//if log contains higher term's log,that means this peer's RaftLog is newer, so reject this election
	if lastLogTerm > m.LogTerm {
		return reject
	}
	//if logTerm equals, but log index is higher, that means in the same term, this peer revieve newer log,so reject this election
	if lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
		return reject
	}

	//do not reject this election,support it
	reject = false
	return reject
}

// handleRequestVote
func (r *Raft) handleRequestVote(m pb.Message) {
	if r.rejectVote(m) {
		r.sendRequestVoteResp(m.From, true)
		return
	}

	// vote for this leader election
	r.Vote = m.From
	r.sendRequestVoteResp(m.From, false)
}

//sendRequestVoteResp
func (r *Raft) sendRequestVoteResp(to uint64, reject bool) {
	msg := pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	r.send(msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// handleRequestVoteResp
func (r *Raft) handleRequestVoteResp(m pb.Message) {
	//maybe this candiate reveive higher term and become follower, so drop all its electionResp msg
	if r.Term > m.Term {
		return
	}

	reject := m.Reject
	if reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}

	votesGrant := 0
	for _, supportVote := range r.votes {
		if supportVote {
			votesGrant++
		}
	}

	if votesGrant > (len(r.Prs) / 2) {
		r.becomeLeader()
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	}
	r.msgs = append(r.msgs, m)
}