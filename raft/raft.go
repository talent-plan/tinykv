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

	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/log"
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

	Peers []uint64

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
	raft := &Raft{
		id:                    c.ID,
		State:                 StateFollower,
		Lead:                  None,
		Peers:                 c.peers,
		Prs:                   make(map[uint64]*Progress),
		RaftLog:               newLog(c.Storage),
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
	if c.Storage != nil {
		hardState, _, _ := c.Storage.InitialState()
		raft.Term = hardState.Term
		raft.Vote = hardState.Vote
		raft.RaftLog.committed = hardState.Commit
		// raft.RaftLog.entries =
	}
	return raft
}

// 选举时间为（config传入的选举时间 , 2*config传入的选举时间）
func (r *Raft) newRandomElectionTimeout() int {
	return r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) getSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) getHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) getLastLogIndex() uint64 {
	return r.RaftLog.LastIndex()
}

func (r *Raft) getLastLogTerm() uint64 {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	return lastTerm
}

func (r *Raft) bcastAppend() {
	if r.State != StateLeader {
		return
	}
	for _, peer := range r.Peers {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) appendEntry(entries ...pb.Entry) {
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = r.getLastLogIndex() + 1 + uint64(i)
	}
	r.RaftLog.append(entries...)

	lastLogIndex := r.getLastLogIndex()
	r.Prs[r.id].Next = lastLogIndex + 1
	r.Prs[r.id].Match = lastLogIndex

	if len(r.Peers) == 1 {
		r.RaftLog.committed = lastLogIndex
		return
	}
}

func (r *Raft) advance(rd Ready) {
	if len(rd.Entries) > 0 {
		r.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if len(rd.CommittedEntries) > 0 {
		r.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// index of log entry immediately preceding new ones
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		panic(err)
	}

	entries := make([]*pb.Entry, 0)
	for i := prevLogIndex + 1; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, r.RaftLog.getItemByIndex(i))
	}

	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Commit:  r.RaftLog.committed,
		Entries: entries,
	}

	r.msgs = append(r.msgs, *msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {

	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.getLastLogIndex(), //记录收到的log entry，最新的index。只有reject == false即添加entry成功，这个变量才会被使用
		Commit:  r.RaftLog.committed,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, *msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// Index:   r.getLastLogIndex(),
		// LogTerm: r.getLastLogTerm(),
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, *msg)
}
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.getLastLogIndex(),
		LogTerm: r.getLastLogTerm(),
		Commit:  r.RaftLog.committed,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) sendRequestVote(to uint64, lastLogIndex, LastLogTerm uint64) {
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: LastLogTerm,
		Index:   lastLogIndex,
	}
	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, *msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
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

	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Infof("[election][err]: %v", err)
		}
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Infof("[heartbeat][err]: %v", err)
		}
	}

}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	log.Infof("[raft]: %d became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		log.Panicf("[raft][error]: %d from leader convert to candidate", r.id)
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	log.Infof("[raft]: %d became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	lastLogIndex := r.getLastLogIndex()

	for _, peer := range r.Peers {
		r.Prs[peer] = &Progress{
			Next: lastLogIndex + 1,
		}
	}
	emptyEntry := pb.Entry{Data: nil}
	// 新加一个空emptyEntry，对于leader增加其Next和Match
	r.appendEntry(emptyEntry)
	r.bcastAppend()
	log.Infof("[raft]: %d became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 通过这步能将Term统一
	// log.Infof("msg: %v", m)

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:

	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:

	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgPropose:

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:

	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:

	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgBeat:
		r.startHeartBeat()
	case pb.MessageType_MsgHeartbeat: //leader也有可能收到心跳
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)

	}
	return nil
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	r.electionElapsed = 0

	r.randomElectionTimeout = r.newRandomElectionTimeout()

	if len(r.Peers) == 1 {
		r.becomeLeader()
		return
	}

	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	for _, peer := range r.Peers {
		if peer != r.id {
			r.sendRequestVote(peer, lastLogIndex, lastLogTerm)
		}
	}
}

func (r *Raft) startHeartBeat() {
	r.heartbeatElapsed = 0
	for _, peer := range r.Peers {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term || r.State == StateLeader || r.State == StateCandidate {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	if lastLogTerm > m.LogTerm || lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.newRandomElectionTimeout()
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	total_votes := len(r.votes)
	majority := len(r.Peers) / 2
	cnt := 0
	for _, vote := range r.votes {
		if vote {
			cnt++
		}
	}
	if cnt > majority {
		r.becomeLeader()
	} else if total_votes-cnt > majority {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	//to do,leader 处理msgPropose
	switch r.State {
	case StateLeader:
		{
			entries := m.Entries
			for _, entry := range entries {
				r.appendEntry(*entry)
			}
			r.bcastAppend()
		}
	case StateFollower:
		{
			// TODO
			// When passed to follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send
			// method. It is stored with sender's ID and later forwarded to the leader by rafthttp package.
			// if r.Lead != None {
			// 	m.To = r.Lead
			// 	r.msgs = append(r.msgs, m)
			// }
		}

	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateCandidate && m.Term >= r.Term {
		r.becomeFollower(r.Term, m.From)
	}
	// TODO 逻辑实现
	r.Lead = m.From

	if r.getLastLogIndex() < m.Index {
		r.sendAppendResponse(m.From, true)
		return
	}
	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	if r.getLastLogIndex() >= m.Index && term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	//开始附加entry，如果有不符合的，直接删除
	for _, entry := range m.Entries {
		if entry.Index > r.getLastLogIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else {
			if en := r.RaftLog.getItemByIndex(entry.Index); en.Term != entry.Term {
				r.RaftLog.truncateAllAfterIndex(en.Index - 1)
				r.RaftLog.stabled = min(uint64(en.Index-1), r.RaftLog.stabled)
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
	}
	// offset := 0

	// for i := m.Index + 1; i <= r.RaftLog.LastIndex() && offset < len(m.Entries); i++ {
	// 	if (*r.RaftLog.getItemByIndex(i)).Index != m.Entries[offset].Index ||
	// 		(*r.RaftLog.getItemByIndex(i)).Term != m.Entries[offset].Term {
	// 		r.RaftLog.truncateAllAfterIndex(i - 1)
	// 		r.RaftLog.stabled = min(uint64(i-1), r.RaftLog.stabled)
	// 		break
	// 	}
	// 	offset += 1
	// }

	// for _, entry := range m.Entries[offset:] {
	// 	r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	// }
	if m.Commit > r.RaftLog.committed {
		// 防止空log
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next -= 1
		r.Prs[m.From].Match = 0
		/*retry?*/
		r.sendAppend(m.From)
		return

	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	}
	if r.updateCommit() {
		for k := range r.Prs {
			if k != r.id {
				r.sendAppend(k)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.newRandomElectionTimeout()
	r.Lead = m.From
	r.State = StateFollower
	//更新commit

	// if m.Commit > r.RaftLog.committed && r.getLastLogIndex() == m.Index && r.getLastLogTerm() == m.LogTerm {
	// 	r.RaftLog.committed = min(m.Commit, r.getLastLogIndex())
	// }
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	//判断收到的response，日志是否和leader一致，否则append
	sendMsg := false
	if m.Index != r.getLastLogIndex() {
		sendMsg = true
	} else {
		term, _ := r.RaftLog.Term(m.Index)
		if term != m.LogTerm {
			sendMsg = true
		}
	}

	if sendMsg {
		r.sendAppend(m.From)
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

func (r *Raft) updateCommit() bool {
	if r.State != StateLeader {
		return false
	}

	// midmatch find
	// 若match 1 2 3 4 5, midMatch = 3,  3 4 5 all >= 3, 即committed = 3
	var list []uint64
	for i, v := range r.Prs {
		if i == r.id {
			v.Match = r.RaftLog.LastIndex()
		}
		list = append(list, v.Match)
	}

	sortkeys.Uint64s(list)
	midMatch := list[(len(list)-1)/2]
	midTerm, _ := r.RaftLog.Term(midMatch)
	if midMatch > r.RaftLog.committed && midTerm == r.Term {
		r.RaftLog.committed = midMatch
		return true
	}
	return false
}
