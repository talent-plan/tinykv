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
	votes     map[uint64]bool
	voteCnt   uint64
	denialCnt uint64

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
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	prs := make(map[uint64]*Progress)
	lastIndex, _ := c.Storage.LastIndex()
	if len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		if c.ID == peer {
			prs[peer] = &Progress{Match: 0, Next: lastIndex + 1}
		} else {
			prs[peer] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		}
	}
	raft := &Raft{
		id:      c.ID,
		Term:    hardState.Term,
		Vote:    hardState.Vote,
		Prs:     prs,
		RaftLog: newLog(c.Storage),
		State:   StateFollower,
		votes:   make(map[uint64]bool),
		// voteCnt: 0,
		// denialCnt: 0,
		msgs:             make([]pb.Message, 0),
		Lead:             hardState.Vote,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// randomElectionTimeout: c.ElectionTick+,
		// heartbeatElapsed: 0,
		// electionElapsed: 0,
		// leadTransferee: 0,
		// PendingConfIndex: 0,
	}
	raft.becomeFollower(raft.Term, None)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

func (r *Raft) boradcastHeartbeat() {
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed), // may not sync commited log
		Term:    r.Term,
	})
}

func (r *Raft) sendVoteRequest(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			// r.becomeCandidate()
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id}); err != nil {
				panic(err)
			}
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// r.boradcastHeartbeat()
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id}); err != nil {
				panic(err)
			}
		}
	}
}

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.resetTick()
	r.Term = term
	r.Lead = lead
	r.Vote = lead

	// r.leadTransferee = None // TODO@wy: to test
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTick()
	r.Term += 1
	r.Vote = r.id
	r.State = StateCandidate
	r.voteCnt = 1
	r.denialCnt = 0
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// for peer := range r.Prs {
	// 	r.sendVoteRequest(peer)
	// }
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.resetTick()
	r.Lead = r.id
	r.State = StateLeader

	// temporarily commet in test2aa
	// r.RaftLog.appendEntries(pb.Entry{
	// 	EntryType: pb.EntryType_EntryNormal,
	// 	Term:      r.Term,
	// 	Index:     r.RaftLog.LastIndex() + 1,
	// 	Data:      nil,
	// })

	r.logSync()
}

func (r *Raft) logSync() {
	lastIndex, _ := r.RaftLog.storage.LastIndex()
	for _, p := range r.Prs {
		p.Match = None //TODO@wy
		p.Next = lastIndex + 1
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.Hup()
	case pb.MessageType_MsgRequestVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := (r.Vote == m.From) ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.Lead == None) ||
			// vote and lager term
			(m.Term > r.Term)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			})
			r.Vote = m.From
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	default:
		switch r.State {
		case StateFollower:
			err := r.stepFollower(m)
			if err != nil {
				return err
			}
		case StateCandidate:
			err := r.stepCandidate(m)
			if err != nil {
				return err
			}
		case StateLeader:
			err := r.stepLeader(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Raft) Hup() {
	if r.State == StateLeader {
		return
	}
	r.campaign()
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: r.RaftLog.LastTerm(),
			})
		}
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From //  have a new lead
		r.handleHeartbeat(m)
		// case pb.MessageType_MsgRequestVote:

	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		if _, ok := r.votes[m.From]; ok {
			return nil
		}
		r.votes[m.From] = !m.Reject
		if m.Reject {
			r.denialCnt += 1
			if r.denialCnt >= uint64(len(r.Prs)/2+1) {
				r.becomeFollower(r.Term, None) // reuse r.Term , m.From may not be leader
			}
		} else {
			r.voteCnt += 1
			if r.voteCnt >= uint64(len(r.Prs)/2+1) {
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // already have a leader , exit election
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.boradcastHeartbeat()
		return nil
	// case pb.mes

	case pb.MessageType_MsgHeartbeatResponse:

	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	})
	// switch r.State {
	// case StateFollower:
	// 	if r.Lead != m.From || r.Term != m.Term {
	// 		r.msgs = append(r.msgs, pb.Message{
	// 			MsgType: pb.MessageType_MsgHeartbeatResponse,
	// 			From:    r.id,
	// 			To:      m.From,
	// 			Term:    r.Term,
	// 			LogTerm: r.RaftLog.LastTerm(),
	// 			Index:   r.RaftLog.LastIndex(),
	// 			Reject:  true,
	// 		})
	// 	} else {
	// 		r.resetTick()
	// 	}
	// case StateCandidate:
	// case StateLeader:
	// }
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
