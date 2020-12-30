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

// Update 更新 follower 的同步进度
func (p *Progress) Update(n uint64) bool {
	var suc bool
	if p.Match < n {
		p.Match = n
		suc = true
	}
	p.Next = max(p.Next, n+1)
	return suc
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64 // 每个 Raft node 有一次投票机会，Vote 为投给的 peer

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool // 接受投票的结果

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
	// number of ticks since it reached last electionTimeout
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
	prs := make(map[uint64]*Progress, 0)
	for _, pr := range c.peers {
		prs[pr] = nil
	}

	raft := &Raft{
		id:               c.ID,
		votes:            make(map[uint64]bool, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs:              prs,
		RaftLog: &RaftLog{
			storage: c.Storage,
		},
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// sendRequestVote 请求投票
func (r *Raft) sendRequestVote(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// 如果 election timemout，重新发起 election，编程 candidate
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.startElection()
		}
	case StateCandidate:
		// candidate 的 electionElapsed 还是会增加，直到出现 leader 才复位
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.startElection()
		}
	case StateLeader:
		// 只要超过 heartbeatTimeout 就发送心跳
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for _, pr := range r.restPeers() {
				r.sendHeartbeat(pr)
			}
		}
	}
}

// 所有的 peers
func (r *Raft) allPeers() []uint64 {
	var ret []uint64
	for pr, _ := range r.Prs {
		ret = append(ret, pr)
	}
	return ret
}

// restPeers 非自己的其他 peers
func (r *Raft) restPeers() []uint64 {
	var ret []uint64
	for pr, _ := range r.Prs {
		if pr != r.id {
			ret = append(ret, pr)
		}
	}
	return ret
}

// quorum 法定数，超半数
func (r *Raft) quorum() int {
	total := len(r.allPeers())
	return total/2 + 1
}

// proVotePeers 选举投票同意的 peers
func (r *Raft) proVotePeers() []uint64 {
	var ret []uint64
	for pr, vote := range r.votes {
		if vote {
			ret = append(ret, pr)
		}
	}
	return ret
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("cannot transit from leader to candidate")
	}
	r.State = StateCandidate
	// Term + 1
	r.Term += 1
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("cannot transit from follower to leader")
	}
	r.State = StateLeader
	r.Lead = r.id

}

// startElection 开始新一轮选举
func (r *Raft) startElection() {
	// 先投自己一票再说
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  false,
	})
	restPeers := r.restPeers()
	// 然后给其他 peers 请求投票
	for _, pr := range restPeers {
		r.sendRequestVote(pr)
	}
	// reset electionElapsed
	r.electionElapsed = 0
}

// recVote 接受投票
func (r *Raft) recVote(m pb.Message) {
	// if !m.Reject {
	// 	r.Vote += 1
	// }
	r.votes[m.From] = !m.Reject
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 每收到一条消息会走一次 Step，不同 State 对不同的消息类型处理方式不同
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term > r.Term:
		r.largerTermStep(m)
	case m.Term < r.Term:
		r.smallerTermStep(m)
	case m.Term == r.Term:
		r.equalTermStep(m)
	}
	return nil
}

func (r *Raft) equalTermStep(m pb.Message) {
	switch r.State {
	case StateLeader:
		r.eTermLeaderStep(m)
	case StateCandidate:
		r.eTermCandidateStep(m)
	case StateFollower:
		r.eTermFollowerStep(m)
	}
	return
}

func (r *Raft) eTermLeaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat: // 心跳前消息
		// 发送心跳
	case pb.MessageType_MsgAppendResponse: // Append 回执消息
		// 检查法定人数并 apply log
	case pb.MessageType_MsgPropose: // leader 写自己 log 的消息
		var entries []pb.Entry
		for _, entry := range m.Entries {
			entries = append(entries, *entry)
		}
		r.appendEntry(entries...)
	}

	// pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeatResponse: // 心跳回执
		// 简单点，暂时不发送心跳回执
		// From 的 Match 是否滞后于 LastIndex，如果滞后，则 sendAppend
	}
}

// appendEntry 负责添加 log entry
func (r *Raft) appendEntry(es ...pb.Entry) bool {
	lastIndex := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + uint64(i) + 1
	}
	lastIndex = r.RaftLog.append(es...)
	// 更新进度
	prog := r.Prs[r.id]
	prog.Update(lastIndex)
	// 提交 raft log(即更新 committed 点)
	r.Prs[r.id] = prog
	r.Commit()
	return true
}

func (r *Raft) Commit() bool {
	r.RaftLog.committed = r.Prs[r.id].Match
	return true
}

func (r *Raft) eTermCandidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse: // candidate 收到投票回执
		r.recVote(m)
		// 判断是否可以成为 leader，判断是否超过半数
		// 不用非得等所有投票的返回
		if len(r.proVotePeers()) >= r.quorum() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgAppend:
		// 如果 candidate 收到 append message，变回 follower
		r.becomeFollower(m.Term, m.From)
	}
}

func (r *Raft) eTermFollowerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		})
		// 简单点，暂时不发送心跳回执
	case pb.MessageType_MsgHup: // 新一轮选举
		// 变成 candidate
		r.becomeCandidate()
		r.startElection()
	case pb.MessageType_MsgRequestVote: // 投票请求
		// 每个 follower 只能投一票，同一个 From 可以重复投（反正是用 hash 记录的）
		canVote := r.Vote == m.From ||
			(r.Vote == None && r.Lead == None)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  !canVote,
		})
	}
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) largerTermStep(m pb.Message) {
	// 无论是什么消息类型，因为 m.Term 大，都变回 follower
	switch r.State {
	case StateLeader:
		// 变回 follower
		r.becomeFollower(m.Term, m.From)
	case StateCandidate:
		// 变回 follower
		r.becomeFollower(m.Term, m.From)
	case StateFollower:
	}
	// 如果消息 Term 比我大，那我就应该变成 From 的 follower，无论是什么消息类型
	r.becomeFollower(m.Term, m.From)
}

func (r *Raft) smallerTermStep(m pb.Message) {
	switch r.State {
	case StateLeader:
	case StateCandidate:
	case StateFollower:
	}
	// TODO 如果消息类型比我小，而且是 leader 发来的消息，我得发消息通知他该更新 Term 了
	// 意外情况，先不着急处理
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
