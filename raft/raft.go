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

	log "github.com/sirupsen/logrus"

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

// Progress represents a follower’s progress in the view of the leader.
// Leader maintains
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	// new raft log
	raftLog := newLog(c.Storage)
	prsMap := make(map[uint64]*Progress, len(c.peers))
	for _, peerId := range c.peers {
		prsMap[peerId] = &Progress{
		}
	}
	return &Raft{
		id:   c.ID,
		Term: 0,
		Prs:  prsMap,
		// should be always start up as a follower state
		State:            StateFollower,
		RaftLog:          raftLog,
		votes:            make(map[uint64]bool, len(c.peers)),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msgReceiverPrs, ok  := r.Prs[to]
	if !ok {
		return false
	}

	m := pb.Message{}
	// next - 1 or next ?
	term, err := r.RaftLog.Term(msgReceiverPrs.Next )
	entryToSend := r.RaftLog.fetchEntries(msgReceiverPrs.Next, 50)
	msgToSend := pb.Message{
		To:	to,
	}

	if err != nil {
		// TODO: snapshot
		// should send snapshot, if we fail to match index
	} else {
		msgToSend.MsgType = pb.MessageType_MsgAppend
		msgToSend.Index = msgReceiverPrs.Next - 1
		m.LogTerm = term
		m.Entries = copyEntry(entryToSend)
		m.Commit = r.RaftLog.committed
	}

	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.msgs = append(
		r.msgs,
		pb.Message{
			From: r.id,
			Term: r.Term,
			To: to,
			Commit: commit,
			MsgType: pb.MessageType_MsgHeartbeat,
		})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++

	// TODO: we don't have to generate randomized timeout everytime
	randomizedElectionTimeout := r.getRandomizedElectionTimeout()
	if r.electionElapsed >= randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		// etcd 中的 checkQuorum 是什么意思
		// abort leader transfer
		// if r.State == StateLeader  {}
	}

	// The leader's duty is to send heartbeat message when tick is invoked
	// NOTE: every tick leader should send heart heartbeat to its followers
	if r.State == StateLeader {
		// if r.heartbeatElapsed >= r.heartbeatTimeout {}
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHeartbeat})
		if err != nil {
			log.WithError(err).Errorf("[tick] Step err with id:%d", r.id)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// TODO: no validate yet.

	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// step1: increase term
	r.Term += 1
	// step3: vote for itself
	r.resetVotes()
	r.voteFor(r.id, true)
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// 怎么增加 entry ?
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	// 这种状态应该是 follower，leader, candidates 都共同需要处理的
	case pb.MessageType_MsgRequestVote:
		// TODO: There will be more restrction here
		// 需要处理情况

		canVote := r.Vote == m.From ||
			(r.Vote == None && r.Lead == None) ||
			(m.MsgType == pb.MessageType_MsgRequestVote && m.Term > r.Term)

		if canVote {
			// TODO: 应该不是在这里判断的才对？
			r.becomeFollower(m.Term, None)
		}
		r.Vote = m.From
		r.send(pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject: !canVote,
		})
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			if r.State != StateFollower {
				r.becomeFollower(m.Term, m.From)
			}
			r.Term = m.Term
			r.Lead = m.From
		}
	}

	// Mimic etcd/raft.go Step function
	switch r.State {
	case StateFollower:
		// follower's responsibility
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.campaignForLeader()
			r.requestVotesFromPeers()
			// log.Infof("ignoring MsgHup because already campaigning current state:%+v", r.State)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 和 follow 重复了，需要抽一个方法出来吗？
			r.becomeCandidate()
			r.campaignForLeader()
			r.requestVotesFromPeers()
		case pb.MessageType_MsgRequestVoteResponse:
			r.recordVote(m)
			r.campaignForLeader()
		case pb.MessageType_MsgBeat:
			// 忽略
		default:
			r.Term = m.Term
			r.becomeFollower(m.Term, m.From)
			r.msgs = append(r.msgs, m)
		}
	case StateLeader:
		// TODO: 这个判断太简单了，需要重构优化一下
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for peerId, _ := range r.Prs {
				if peerId == r.id {
					continue
				}
				r.sendHeartbeat(peerId)
			}
		case pb.MessageType_MsgPropose:
			// 1. appendEntry()
			// 2. broadcastAppend()
			if len(m.Entries) == 0 {
				// TODO: should return a understandable error
				return nil
			}
			if _, ok := r.Prs[r.id]; !ok {
				return ErrStepPeerNotFound
			}
			if !r.appendEntry(m.Entries...) {
				return ErrProposalDropped
			}

			r.broadcastAppend()
		default:
		}
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

func (r *Raft) requestVotesFromPeers() {
	if r.State != StateCandidate {
		return
	}
	for peerId, _ := range r.Prs {
		if r.id == peerId {
			continue
		}
		r.send(pb.Message{
			From:    r.id,
			Term:    r.Term,
			To:      peerId,
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) campaignForLeader() {
	granted, rejected, res := r.TallyVotes()
	switch res {
	case VoteWon:
		r.becomeLeader()
	case VoteLost:
		// TODO: 当选举失败的时候，需要从 candidate 变成 follower
		// 并且进行一些清理操作
		// r.becomeFollower()
	default:
	}
	_ = granted
	_ = rejected
	// log.Infof("node:%d campaignForLeader granted:%d rejected:%d res:%+v", r.id, granted, rejected, res)
}

func (r *Raft) voteFor(id uint64, accepted bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = accepted
		// log.Infof("id:%d voteFor %d", id, r.id)
	} else {
		// log.Infof("node %d duplicate vote for id:%d", r.id, id)
	}
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool, len(r.Prs))
	r.Vote = 0
	// log.Infof("id:%d reset vote at term:%d current state:%d", r.id, r.Term, r.State)
	return
}

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

// From etcd
// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).

func (r *Raft) TallyVotes() (granted int, rejected int, res VoteResult) {
	for peerId, _ := range r.Prs {
		v, voted := r.votes[peerId]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}

	// NOTE: majority = len(r.Prs)/2 + 1 , instead of len(r.Prs) / 2
	// Reason
	majorityNum := len(r.Prs) / 2 + 1
	switch {
	// won the campaign, if the granted num exceeds the majority num.
	case granted >= majorityNum:
		return granted, rejected, VoteWon
	case granted+rejected >= majorityNum:
		return granted, rejected, VoteLost
	// haven't receive the vote resp from the majority yet
	default:
		return granted, rejected, VotePending
	}
}

func (r *Raft) recordVote(m pb.Message) {
	r.voteFor(m.From, !m.Reject)
	// log.Infof("node:%d recordVote %+v", r.id, m)
}

// inspired by etcd send
func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}


func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// 为什么需要加 1 ？
func (r *Raft) getRandomizedElectionTimeout() int {
	return r.electionTimeout + rand.Intn(r.electionTimeout) + 1
}

func (r *Raft) appendEntry(es ...*pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}

	if after := es[0].Index - 1; after < r.RaftLog.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, r.RaftLog.committed)
	}

	currentLastIndex:= r.RaftLog.LastIndex()
	// tracking the progress of itself
	// see etcd MaybeUpdate
	pr, ok := r.Prs[r.id]
	if ok {
		if pr.Match < currentLastIndex {
			pr.Match = currentLastIndex
		}
		if pr.Next < currentLastIndex + 1 {
			pr.Next = currentLastIndex + 1
		}
	}

	return true
}

func (r *Raft) broadcastAppend() {
	for peerId, _ := range r.Prs {
		if r.id == peerId {
			continue
		}
		r.sendAppend(peerId)
	}
}
