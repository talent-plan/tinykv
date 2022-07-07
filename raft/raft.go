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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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

	randomedElectionTimeout int
	actives                 map[uint64]bool
	//for debug
	t *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardstate, confstate, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	//peer information is already included in the storage
	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	raftlog := newLog(c.Storage)

	if c.Applied > 0 {
		raftlog.applied = c.Applied
	}

	prs := make(map[uint64]*Progress)
	lastIndex := raftlog.LastIndex()
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}

	raft := &Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
		RaftLog:          raftlog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		//will be used in 2c
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	raft.t = log.New()
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	prevIndex := r.Prs[to].Next - 1
	println(prevIndex)
	prevTerm, _ := r.RaftLog.Term(prevIndex)
	firstEntryIndex := r.RaftLog.FirstEntryIndex()
	lastIndex := r.RaftLog.LastIndex()
	//lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	entries := []*pb.Entry{}
	for i := prevIndex + 1; i <= lastIndex; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstEntryIndex])
	}
	//r.t.Errorf("%d,%d", lastIndex, lastLogTerm)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	for _, m := range r.msgs {
		r.t.Errorf("%v", m)
	}
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) bool {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	if _, ok := r.Prs[to]; !ok {
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	//r.t.Errorf("%d", lastIndex)
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return
	}
	//println("send -- m.From:", r.id, "m.To:", to, "m.Term:", r.Term, "m.Index:", lastIndex, "m.LogTerm:", lastLogTerm)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
	}
	r.msgs = append(r.msgs, msg)
	//r.t.Errorf("ok")
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	t := log.New()
	t.Debugf("r.id:%d, r.state:%s", r.id, r.State)
	switch r.State {
	case StateFollower:
		if err := r.stateFollowerTick(); err != nil {
			panic(err)
		}
	case StateCandidate:
		if err := r.stateCandidateTick(); err != nil {
			panic(err)
		}
	case StateLeader:
		if err := r.stateLeaderTick(); err != nil {
			panic(err)
		}
	}
}
func (r *Raft) stateFollowerTick() error {
	r.electionElapsed++
	if r.electionElapsed >= r.randomedElectionTimeout {
		r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		if err := r.Step(pb.Message{
			From:    r.id,
			MsgType: pb.MessageType_MsgHup,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (r *Raft) stateCandidateTick() error {
	r.electionElapsed++
	if r.electionElapsed >= r.randomedElectionTimeout {
		r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		if err := r.Step(pb.Message{
			From:    r.id,
			MsgType: pb.MessageType_MsgHup,
		}); err != nil {
			return err
		}
	}
	return nil
}
func (r *Raft) stateLeaderTick() error {
	r.electionElapsed++
	if r.electionElapsed >= r.randomedElectionTimeout {
		activeCnt := len(r.actives)
		allCnt := len(r.Prs)
		r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.actives = make(map[uint64]bool)
		r.actives[r.id] = true
		if activeCnt <= allCnt/2 {
			if err := r.Step(pb.Message{
				From:    r.id,
				MsgType: pb.MessageType_MsgHup,
			}); err != nil {
				return err
			}
		}
	}
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			return err
		}
	}
	return nil
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
	//r.raiseCampaign()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.leadTransferee = None
	r.Lead = r.id
	r.actives = make(map[uint64]bool)
	r.actives[r.id] = true
	lastIndex := r.RaftLog.LastIndex()
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Match = r.Prs[peer].Next
			r.Prs[peer].Next += 1
		} else {
			r.Prs[peer].Next = lastIndex + 1
			r.Prs[peer].Match = 0
			r.sendAppend(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.FollowerStep(m)
	case StateCandidate:
		return r.CandidateStep(m)
	case StateLeader:
		return r.LeaderStep(m)
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		r.raiseCampaign()
	case pb.MessageType_MsgBeat:
		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		r.raiseCampaign()
	case pb.MessageType_MsgBeat:
		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			//r.handleMsgPropose(m)
		}
		// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		//收到更新Term的添加日志，转为follower
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
		// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:

		// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:

		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
		// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.

	case pb.MessageType_MsgBeat:
		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendHeartbeat(peer)
		}
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
		// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
		// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
		// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
		// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		r.actives[m.From] = true
		if m.Commit < r.RaftLog.committed {
			r.sendAppend(m.From)
		}
		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.t.Errorf("111")
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.Term != r.Term {
		r.Lead = m.From
	}
	lastIndex := r.RaftLog.LastIndex()
	if m.Index >= lastIndex {
		r.sendAppendResponse(m.From, true)
		return
	}
	prevLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic("wrong when get preTermLog")
	}
	//match
	if m.LogTerm == prevLogTerm {
		for index, entry := range m.Entries {
			entry.Index = m.Index + 1 + uint64(index)
			if entry.Index <= lastIndex {
				term, _ := r.RaftLog.Term(entry.Index)
				if term != entry.Term {
					r.RaftLog.entries = r.RaftLog.entries[r.RaftLog.FirstEntryIndex():entry.Index]
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
					lastIndex = r.RaftLog.LastIndex()
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		}
		r.Vote = None
		r.t.Errorf("%v", r.RaftLog.entries)
	} else {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.Vote = None
	r.sendAppendResponse(m.From, false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.stabled,
		Commit:  r.RaftLog.committed,
	}
	if m.Term >= r.Term {
		r.Term = m.Term
		r.Lead = m.From
		r.electionElapsed = 0
		r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		msg.Reject = false
	} else {
		msg.Reject = true
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handlePropose(m pb.Message) {
	//r.RaftLog
	for _, entry := range m.Entries {
		lastIndex := r.RaftLog.LastIndex()
		entry.Term = r.Term
		entry.Index = lastIndex + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match, r.Prs[r.id].Next = r.RaftLog.LastIndex(), r.RaftLog.LastIndex()+1
	for peer := range r.Prs {
		if r.id == peer {
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
	}
	if m.Term >= r.Term {
		r.t.Errorf("check1,%d,%d", m.From, r.id)
		if r.Vote == None || r.Vote == m.From {
			lastIndex := r.RaftLog.LastIndex()
			lastTerm, _ := r.RaftLog.Term(lastIndex)
			r.t.Errorf("check2,m.logTerm:%d,last logTerm:%d, m.index:%d, lastIndex:%d", m.LogTerm, lastTerm, m.Index, lastIndex)
			if m.LogTerm > lastTerm ||
				(m.LogTerm == lastTerm && m.Index >= lastIndex) {
				//r.t.Errorf("check3")
				r.Vote = m.From
				if r.Term < m.Term {
					r.Term = m.Term
				}
				msg.Reject = false
			}
		}
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	voteFor, voteAgainst := 0, 0
	r.votes[m.From] = !m.Reject
	for _, v := range r.votes {
		if v {
			voteFor++
		} else {
			voteAgainst++
		}
	}
	//r.t.Errorf("voteFor:%d,Voteagainst:%d\n", voteFor, voteAgainst)
	if voteFor > len(r.Prs)/2 {
		r.becomeLeader()
	} else if voteAgainst > len(r.Prs)/2 {
		r.becomeFollower(m.Term, m.From)
	}
	//r.t.Errorf("check4")
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		if m.Term <= r.Term {
			next := r.Prs[m.From].Next - 1
			r.Prs[m.From].Next = min(m.Index, next)
			r.sendAppend(m.From)
		}
		return
	}
	//r.actives[m.From] = true
	return
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

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.randomedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Vote = None
	r.votes = make(map[uint64]bool)
	for peer := range r.Prs {
		progress := &Progress{
			Match: 0,
			Next:  0,
		}
		if peer == r.id {
			progress.Match = r.RaftLog.LastIndex()
		}
		r.Prs[peer] = progress
	}
}

func (r *Raft) raiseCampaign() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.becomeCandidate()
	//only 1 node
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if r.id == peer {
			continue
		}
		//r.t.Errorf("send vote request begin")
		r.sendRequestVote(peer)
		//r.t.Errorf("send vote request")
	}
	//r.t.Errorf("campaign end")
}
