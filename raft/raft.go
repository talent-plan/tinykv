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
	// 每个peer的进度
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// get hardState & confState
	hardState, confState, _ := c.Storage.InitialState()

	// init peers
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress), // only in leader
		State:            StateFollower,              // init must be follower
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// others is nil
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster.
	for _, peer := range c.peers {
		// Progress 代表与其它节点当前匹配的index和下一个将要发给该节点的indedx
		// match & next
		raft.Prs[peer] = &Progress{Match: 0, Next: raft.RaftLog.LastIndex() + 1}
		// c.Storage.LastIndex
	}

	return raft
}

func (r *Raft) isValid(to uint64) bool {
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// peer is not in cluster
	if !r.isValid(to) {
		return false
	}

	prevIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevIndex)

	if err != nil {
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgSnapshot})
		if err != nil {
			return false
		}
		return false
	}

	ents := make([]*pb.Entry, 0)
	n := r.RaftLog.LastIndex() + 1
	firstIndex := r.RaftLog.FirstIndex()
	for i := prevIndex + 1; i < n; i++ {
		ents = append(ents, &r.RaftLog.entries[i-firstIndex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if !r.isValid(to) {
		return
	}

	// 发送的心跳信息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	// 刚同步心跳差
	r.heartbeatElapsed = 0
	// 距离上次同步选举的时间差
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if !r.isValid(r.id) {
		return
	}
	r.State = StateCandidate
	r.Lead = None
	r.Term = r.Term + 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// 刚同步心跳差
	r.heartbeatElapsed = 0
	// 距离上次同步选举的时间差
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if !r.isValid(r.id) {
		return
	}

	r.State = StateLeader
	r.Lead = r.id

	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	lastIndex := r.RaftLog.LastIndex()

	// 更新prs
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0
	}

	// 更新自己的prs
	r.Prs[r.id].Next++
	r.Prs[r.id].Match = r.Prs[r.id].Next

	// 追加日志
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})

	for peer := range r.Prs {
		if peer != r.id {
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
		err := r.FollowerStep(m)
		if err != nil {
			return err
		}
	case StateCandidate:
		err := r.CandidateStep(m)
		if err != nil {
			return err
		}
	case StateLeader:
		err := r.LeaderStep(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) {

}

func (r *Raft) handleRequestVote(m pb.Message) {

}

func (r *Raft) handleAppendResponse(m pb.Message) {

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	// 用于选举
	case pb.MessageType_MsgHup:
		r.Election()
	case pb.MessageType_MsgBeat:
	// 已经发送心跳信息 leader only
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend: // 复制
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	// leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	// 用于选举
	case pb.MessageType_MsgHup:
		r.Election()
	case pb.MessageType_MsgBeat:
	// 已经发送心跳信息 leader only
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend: // 复制
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	// leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// 拉票回应
		r.votes[m.From] = !m.Reject
		voteNum := len(r.votes)
		length := len(r.Prs) / 2
		if voteNum <= length {
			return nil
		}

		grant, denials := 0, 0
		for _, status := range r.votes {
			if status {
				grant++
			} else {
				denials++
			}
		}
		if grant > denials {
			r.becomeLeader()
		} else if denials > grant {
			r.becomeFollower(r.Term, m.From)
		}

	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	// 用于选举
	case pb.MessageType_MsgHup:
		r.Election()
	case pb.MessageType_MsgBeat:
		// 已经发送心跳信息 leader only
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend: // 复制
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	// leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

func (r *Raft) Election() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}

	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

func (r *Raft) sendRequestVote(to uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: lastTerm,
		Index:   lastIndex,
		Entries: nil,
	}

	r.msgs = append(r.msgs, msg)
	return true

}
