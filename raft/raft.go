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
	"github.com/pingcap/log"
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

	transferElapsed int

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

	baseTimeout int

	voteNumber int

	rejectNumber int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)

	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}

	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := raftLog.LastIndex()
	for _, id := range c.peers {
		if id == c.ID {
			prs[id] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			prs[id] = &Progress{Next: lastIndex + 1}
		}
		votes[id] = false
	}
	return &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		electionTimeout:  c.ElectionTick,
		baseTimeout:      c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Prs:              prs,
		votes:            votes,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
	}
}

// GetHardState 得到当前Raft节点持久化相关信息
func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	index := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(index)

	if err != nil {
		log.Info("get RaftLog.Term failed in sendAppend")
		return false
	}
	// 确定发送的日志的第一条对应的索引,仔细想一下
	lastLogIndex := r.RaftLog.LastIndex()
	offset := lastLogIndex - r.Prs[to].Next
	start := uint64(len(r.RaftLog.entries)-1) - offset
	entries := r.RaftLog.entries[start:]
	var sendEntries []*pb.Entry
	for i := 0; i < len(entries); i++ {
		sendEntries = append(sendEntries, &entries[i])
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).\
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// requestElection Follower进入选举流程，申请选举
// Follower->Candidate，同时请求投票
func (r *Raft) requestElection() {
	r.becomeCandidate()
	// 如果只是单个Raft节点，则直接变为leader，主要是要pass TestLeaderElectionInOneRoundRPC2AA的case1
	if len(r.Prs) == 1 {
		r.votes[r.id] = true
		r.voteNumber += 1
		r.becomeLeader()
		return
	}
	r.votes[r.id] = true
	r.voteNumber += 1
	index := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(index)
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   index,
			LogTerm: logTerm,
		})
	}
	r.electionTimeout = r.baseTimeout + rand.Intn(r.baseTimeout)
	r.electionElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		// Leader心跳超时，向所有Follower发起心跳
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			r.heartbeatElapsed = 0
		}
	} else if r.State == StateCandidate {
		r.electionElapsed += 1
		// Candidate选举超时
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			})
		}
	} else {
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.voteNumber, r.rejectNumber = 0, 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.State = StateCandidate
	r.voteNumber, r.rejectNumber = 0, 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	// 称为leader后初始化所有节点的next数组
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	}
	r.voteNumber, r.rejectNumber = 0, 0
	// 开启发起日志replication操作
	r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			//todo
		case pb.MessageType_MsgTransferLeader:
			//todo
		case pb.MessageType_MsgTimeoutNow:
			//todo
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		// 超时重新选举的情况
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			//todo
		case pb.MessageType_MsgTransferLeader:
			//todo
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgPropose:
			// is not transferring leader
			if r.leadTransferee == None {
				r.handlePropose(m)
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgSnapshot:
			//todo
		case pb.MessageType_MsgTransferLeader:
			//todo
		}
	}
	return nil
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// todo
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// todo:2AB
	if m.Term > r.Term {
		//退回follower，变无主状态
		r.becomeFollower(m.Term, 0)
	}
	lastIndex := r.RaftLog.LastIndex()
	Term, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		log.Info("out of bounds index when handleHearBeatResponse")
		return
	}
	if lastIndex != m.Index || Term != m.LogTerm {
		//此时Follower的日志需要补齐
		r.sendAppend(m.From)
	}
}
func (r *Raft) AppendEntry(entries []*pb.Entry) {
	for _, entry := range entries {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		if len(r.Prs) == 1 {
			r.RaftLog.committed += 1
		}
	}
}

func (r *Raft) BcastEntry() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}
func (r *Raft) handlePropose(m pb.Message) {
	// todo:2AB
	// Leader：先AppendEntry到自己的logEntry里，后BoardCast给follower做同步
	if r.State == StateLeader {
		r.AppendEntry(m.Entries)
		//更新自己的match和next index
		r.Prs[r.id].Next += uint64(len(m.Entries))
		r.Prs[r.id].Match += uint64(len(m.Entries))
		// BoardCast
		r.BcastEntry()
		return
	}
	if r.State == StateCandidate {
		return
	}
	// follower将请求转发给Leader
	if r.State == StateFollower {
		forwardPropose := pb.Message{
			MsgType: pb.MessageType_MsgPropose,
			From:    r.id,
			To:      r.Lead,
			Entries: m.Entries,
		}
		r.msgs = append(r.msgs, forwardPropose)
		return
	}

}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// todo:2AB
	// 逐一回退
	if m.Reject && m.Index == r.Prs[m.From].Next-1 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	// 十分重要：Never commit log from previous term and handle response once
	term, _ := r.RaftLog.Term(m.Index)
	if term != r.Term || m.Index < r.Prs[m.From].Next {
		return
	}
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	//log.Info(strconv.FormatUint(m.Index, 10))
	if m.Index > r.RaftLog.committed {
		r.detectCommit()
	}

}

func (r *Raft) detectCommit() {
	for r.RaftLog.committed < r.RaftLog.LastIndex() {
		nextCommit := r.RaftLog.committed + 1
		commitNum := 1
		for id, v := range r.Prs {
			if id == r.id {
				continue
			}
			if v.Match >= nextCommit {
				commitNum += 1
			}
		}
		if commitNum > len(r.Prs)/2 {
			r.RaftLog.committed += 1
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
		} else {
			break
		}
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	//TestLeaderElectionInOneRoundRPC2AA case1特判，只有一个节点时，自己投完票
	if len(r.votes) == 1 {
		r.becomeLeader()
	}
	// 不在一轮投票内，直接返回
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	// 如果成功给票，则实时统计看能否达到一半票数晋级leader
	if !m.Reject {
		r.voteNumber += 1
		if r.voteNumber > len(r.votes)/2 {
			r.becomeLeader()
			for id := range r.votes {
				r.votes[id] = false
			}
		}
	} else {
		// 如果不给票，则实时统计看能否达到一半人拒接，则退回Follower
		r.rejectNumber += 1
		if r.rejectNumber > len(r.votes)/2 {
			r.becomeFollower(r.Term, r.Lead)
			for id := range r.votes {
				r.votes[id] = false
			}
		}
	}
}

func (r *Raft) handleVote(m pb.Message) {
	voteRes := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if r.Vote != 0 && (r.Vote != m.From) {
		// 已经给别人投了票
		voteRes.Reject = true
		voteRes.Term = r.Term
		r.msgs = append(r.msgs, voteRes)
		return
	}
	if m.Term > r.Term {
		// 可以理解为将Follower的投票Term更新，但这时还没有真正意义上投票
		r.Term = m.Term
		r.Vote = None
		r.State = StateFollower
	}

	if m.Term < r.Term {
		// Candidate Term比自己小，任期自己新，不给票
		voteRes.Reject = true
		voteRes.Term = r.Term
		r.msgs = append(r.msgs, voteRes)
		return
	}
	// 判断日志是否足够新,此时
	if m.LogTerm < lastTerm {
		voteRes.Reject = true
	} else if m.LogTerm == lastTerm && m.Index < lastIndex {
		voteRes.Reject = true
	} else {
		// 确认投票，并且切换自己的身份
		voteRes.Reject = false
		r.Vote = m.From
		r.Term = m.Term
		r.State = StateFollower
	}

	voteRes.Term = r.Term

	r.msgs = append(r.msgs, voteRes)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	appendEntryResp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
	}
	// 先判断是否在同一个任期
	// todo：2AB 完成日志复制Resp回复相关逻辑
	if m.Term < r.Term {
		appendEntryResp.Term = r.Term
		appendEntryResp.Reject = true
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}

	r.Term = m.Term
	appendEntryResp.Term = r.Term
	r.Lead = m.From
	// 判断待复制日志是否匹配上
	term, err := r.RaftLog.Term(m.Index)
	if term != m.LogTerm || err != nil {
		//这里可以优化，目前是针对nextIndex逐一回退
		appendEntryResp.Reject = true
		appendEntryResp.Index = m.Index
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	// 从这里开始进行实际的日志复制
	// 特判传过来的日志为空时的情况
	// 1.当传过来的日志为空时，只需要更新commit index
	if len(m.Entries) == 0 {
		if m.Commit > r.RaftLog.committed {
			// 注意这里的细节，仔细想一下为什么这么写
			// 当follower节点远远落后leader节点时，可能存在绝大多数节点已经对index 5完成多数复制，但是这个节点的日志刚追赶到index 3
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
		appendEntryResp.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	var first uint64
	// 日志覆盖或追加
	if len(r.RaftLog.entries) != 0 {
		first = r.RaftLog.entries[0].Index
	} else {
		first, _ = r.RaftLog.storage.FirstIndex()
	}

	// 当前节点日志比leader新，则leader会强制覆盖这个节点
	// todo
	//if first > m.Entries[0].Index {
	//	m.Entries = m.Entries[first-m.Entries[0].Index:]
	//}
	offset := m.Entries[0].Index - first

	for _, entry := range m.Entries {
		if offset == uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if entry.Term != r.RaftLog.entries[offset].Term || entry.Index != r.RaftLog.entries[offset].Index {
			r.RaftLog.entries = append([]pb.Entry{}, r.RaftLog.entries[:offset]...)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			if entry.Index <= r.RaftLog.stabled {
				r.RaftLog.stabled = entry.Index - 1
			}
		}
		offset += 1
	}
	appendEntryResp.Index = r.RaftLog.LastIndex()
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, appendEntryResp.Index)
	}
	r.msgs = append(r.msgs, appendEntryResp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	hbResp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	// 有可能因为网络分区，旧主发来心跳消息，告诉旧主你是旧的
	// todo:
	// 这里在仔细想一下怎么处理，不同处理方式是否有影响？旧主发来心跳，当前Follower是告诉他你是旧主，还是直接不给回复？接收到旧主的心跳自己的electionElapsed到底要不要更新
	// 目前是旧主心跳不更新electionElapsed，但告诉旧主你是旧的
	if m.Term < r.Term {
		hbResp.Term = r.Term
		hbResp.Reject = true
		r.msgs = append(r.msgs, hbResp)
		return
	}
	r.State = StateFollower
	r.electionElapsed = 0
	r.Term = m.Term
	r.Lead = m.From
	hbResp.Index = r.RaftLog.LastIndex()
	hbResp.LogTerm, _ = r.RaftLog.Term(hbResp.Index)
	r.msgs = append(r.msgs, hbResp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// todo
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	// todo
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	// todo
}
