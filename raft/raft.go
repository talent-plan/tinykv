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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
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

type lockedRand struct {
	sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.Lock()
	defer r.Unlock()
	v := r.rand.Intn(n)
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
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
	// create a new raft log
	raftLog := newLog(c.Storage)

	// get hard`State & confState
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	// init peers
	// 我们目前的test都是指定集群节点的
	if c.peers == nil {
		c.peers = cs.Nodes
	}

	raft := &Raft{
		id:      c.ID,
		RaftLog: raftLog,
		Prs:     make(map[uint64]*Progress), // only in leader
		//State:            StateFollower,              // init must be follower
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// others is nil
	}

	if !IsEmptyHardState(hs) {
		raft.loadState(hs)
	}

	if c.Applied > 0 {
		raft.RaftLog.appliedTo(c.Applied)
	}

	raft.becomeFollower(raft.Term, None)

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster.
	for _, peer := range c.peers {
		// Progress 代表与其它节点当前匹配的index和下一个将要发给该节点的indedx
		// match & next
		raft.Prs[peer] = &Progress{Match: 0, Next: raft.RaftLog.LastIndex() + 1}
	}

	return raft
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Printf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *Raft) hasLeader() bool {
	return r.Lead != None
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
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
	return r.maybeSendAppend(to)
}

func (r *Raft) maybeSendAppend(to uint64) bool {

	msg := pb.Message{}
	msg.To = to

	// get last log index
	prevIdx := r.Prs[to].Next - 1
	// get last log term
	prevLogTerm, err := r.RaftLog.Term(prevIdx)

	// NO USE
	if err != nil {
		// TODO: error
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgSnapshot})
		if err != nil {
			return false
		}
		return false
	}

	msg.MsgType = pb.MessageType_MsgAppend
	msg.Index = prevIdx
	msg.LogTerm = prevLogTerm
	msg.Commit = r.RaftLog.committed
	msg.Term = r.Term

	// TODO: need to update get entries ...
	ents := make([]*pb.Entry, 0)
	n := r.RaftLog.LastIndex() + 1
	firstIndex := r.RaftLog.FirstIndex()
	for i := prevIdx + 1; i < n; i++ {
		ents = append(ents, &r.RaftLog.entries[i-firstIndex])
	}

	msg.Entries = ents

	return r.send(msg)
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
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	r.send(msg)

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		r.electionElapsed++
		// 选举过期
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		// 心跳过期
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}
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
	r.electionElapsed = 0 - globalRand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if !r.isValid(r.id) {
		return
	}
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// 刚同步心跳差
	r.heartbeatElapsed = 0
	// 距离上次同步选举的时间差
	r.electionElapsed = 0 - globalRand.Intn(r.electionTimeout)
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
	r.electionElapsed = 0 - globalRand.Intn(r.electionTimeout)

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
func (r *Raft) Step(m pb.Message) (err error) {
	// Your Code Here (2A).
	if !r.isValid(r.id) {
		return
	}
	switch r.State {
	case StateFollower:
		err = r.FollowerStep(m)
	case StateCandidate:
		err = r.CandidateStep(m)
	case StateLeader:
		err = r.LeaderStep(m)
	}
	return
}

// 提案
func (r *Raft) handlePropose(m pb.Message) {
	lastIdx := r.RaftLog.LastIndex()

	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIdx + uint64(i) + 1
		// 直接拼接，不是在原有的基础上修改
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
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
	}

	if m.Term < r.Term {
		msg.Reject = true
		r.send(msg)
		return
	}

	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	if r.Vote == None || r.Vote == m.From {
		if r.Term < m.Term {
			r.Term = m.Term
		}

		lastIdx := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIdx)

		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIdx) {
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}

			// 因为上面更新过，所以这里需要重新写入
			msg.Term = r.Term
			msg.Reject = false

			r.send(msg)
			return
		}
	}

	msg.Reject = true
	r.send(msg)
	return
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject == true {
		// TODO:question,这里在handleAppendEntries并未实现index的写入？默认是零？
		// 已解决：m返回的时候会带有匹配的本地最小index
		r.Prs[m.From].Next = m.Index
		r.sendAppend(m.From)
		return
	}

	// update prs
	r.Prs[m.From].Match = m.Index
	// update next index
	r.Prs[m.From].Next = m.Index + 1

	match := make(uint64Slice, len(r.Prs))
	i := 0
	// TODO:question
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	Match := match[(len(r.Prs)-1)/2]
	if Match > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(Match)
		if logTerm == r.Term {
			r.RaftLog.committed = Match
		}
	}
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	if m.Term < r.Term {
		msg.Reject = true
		msg.Index = 0
		r.send(msg)
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}

	// 更新选举过期时间
	r.electionElapsed -= globalRand.Intn(r.electionTimeout)
	// 获取本地最新index
	lastIdx := r.RaftLog.LastIndex()
	if m.Index <= lastIdx {
		// 校验term
		LogTerm, _ := r.RaftLog.Term(m.Index)
		if LogTerm == m.LogTerm {
			// 两者之间是自己本地的无效信息
			if m.Index < lastIdx {
				// delete log
				firstIdx := r.RaftLog.FirstIndex()
				r.RaftLog.entries = r.RaftLog.entries[:m.Index-firstIdx+1]
			}
			// 追加新日志
			for i, entry := range m.Entries {
				entry.Term = r.Term
				entry.Index = m.Index + uint64(i) + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}

			// 返回追加日志的回应消息
			r.Vote = None
			msg.Reject = false
			msg.Term = r.Term
			msg.Index = m.Index + uint64(len(m.Entries))
			r.send(msg)

			// committed
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			return
		}
	}

	// leader的index比本地的index大，证明中间有日志缺失
	// TODO:交给上层逻辑？？
	// 已解决同H531
	msg.Reject = true
	msg.Index = 0
	r.send(msg)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	if m.Term < r.Term {
		msg.Reject = true
		r.send(msg)
		return
	}

	if m.Term > r.Term {
		r.Term = m.Term
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}

	r.electionElapsed -= globalRand.Intn(r.electionTimeout)
	msg.Reject = true
	msg.Term = r.Term
	r.send(msg)
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

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	// 用于选举
	case pb.MessageType_MsgHup:
		r.Election()
	case pb.MessageType_MsgAppend: // 复制
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	default:
		return errors.New("unsupported MsgType for Follower")
	}

	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	// 用于选举
	case pb.MessageType_MsgHup:
		r.Election()
	case pb.MessageType_MsgAppend: // 复制
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.poll(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	default:
		return errors.New("unsupported MsgType for Candidate")
	}

	return nil
}

// poll 轮询唱票
func (r *Raft) poll(m pb.Message) {
	r.votes[m.From] = !m.Reject
	length := len(r.Prs) / 2

	grant, denials := 0, 0
	for _, status := range r.votes {
		if status {
			grant++
		} else {
			denials++
		}
	}
	if grant > length {
		r.becomeLeader()
	} else if denials > length {
		r.becomeFollower(r.Term, m.From)
	}
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
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
	default:
		return errors.New("unsupported MsgType for Leader")
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
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIdx)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: lastTerm,
		Index:   lastIdx,
		Entries: nil,
	}

	return r.send(msg)
}

func (r *Raft) send(m pb.Message) bool {
	if m.From == None {
		m.From = r.id
	}

	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
			return false
		}
	}

	r.msgs = append(r.msgs, m)
	return true
}
