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
	"sort"
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
	// the node this term vote for
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
	randomizedElectionTimout int
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
	r := &Raft{
		id:c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		randomizedElectionTimout: c.ElectionTick,
		State: StateFollower, // When servers start up, they begin as followers
		Vote: None,
		Prs:make(map[uint64]*Progress),
		votes: make(map[uint64]bool),
		RaftLog: newLog(c.Storage),
	}
	hardState,confStat,_ := r.RaftLog.storage.InitialState()
	if c.peers == nil{
		c.peers = confStat.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _,p := range c.peers {
		r.Prs[p]=&Progress{
			Next: lastIndex+1,
		}
		if r.id == p {
			r.Prs[p].Match = lastIndex
		}
	}
	r.Vote = hardState.GetVote()
	r.Term = hardState.GetTerm()
	r.RaftLog.committed = hardState.GetCommit()
	r.RaftLog.applied = c.Applied
	return r
}
// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.electionElapsed = 0
}
// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	// Increment currentTerm
	r.Term = r.Term +1
	// Vote for self
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	// Reset election timer
	r.electionElapsed = 0
	if  uint64(len(r.Prs)) == 1 {
		r.becomeLeader()
	}

}
// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	for to := range r.Prs{
			r.Prs[to] = &Progress{
				Next: r.RaftLog.LastIndex()+1,
			}
	}
	// append noop entry
	r.appendNoopEntry()
	r.Prs[r.id] = &Progress{
		Next: r.RaftLog.LastIndex()+1,
		Match: r.RaftLog.LastIndex(),
	}
	r.broadcastAppend()

	if len(r.Prs) ==1{
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}


// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term && m.From != r.id{
		r.becomeFollower(m.Term,None)
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
func (r *Raft) stepLeader(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.handleMsgBeat()
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppend:
		// leader send MsgAppend to other followers when it receives local message MsgPropose
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}
func (r *Raft) stepFollower(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}
func (r *Raft) stepCandidate(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		// if a node claim to be a leader which means it win the election,then transfer to it's follower
		if m.Term == r.Term{
			r.becomeFollower(m.Term,m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}


// broadcastAppend broadcast current log info to it's followers
func (r *Raft) broadcastAppend(){
	for to := range r.Prs{
		if to != r.id{
			r.sendAppend(to)
		}
	}
}
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	prevIndex := pr.Next-1
	prevLogTerm ,_:= r.RaftLog.Term(prevIndex)
	ents := r.RaftLog.sliceFrom(prevIndex+1)

	entries := make([]*pb.Entry,0)
	for i := range ents{
		entries = append(entries,&ents[i])
	}
	msg := pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		Entries: entries,
		Index: prevIndex,
		LogTerm: prevLogTerm,
		MsgType: pb.MessageType_MsgAppend,
	}
	r.msgs = append(r.msgs,msg)
	return true
}
func (r *Raft) sendAppendResponse(to ,term , index uint64,reject bool){
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To:to,
		Term: r.Term,
		Reject: reject,
		Index: index,
		LogTerm: term,
	}
	r.msgs = append(r.msgs,msg)
}
// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State != StateLeader{
		return
	}
	msg:=pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
	}
	r.msgs = append(r.msgs,msg)
}
func (r *Raft) sendHeartBeatResponse(to uint64,reject bool){
	msg:=pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To:to,
		Reject: reject,
		Term:r.Term,
	}
	r.msgs = append(r.msgs,msg)
}
// sendRequestVote sends a RequestVote RPC to the given peer
func (r *Raft) sendRequestVote(to uint64){
	if r.State != StateCandidate{
		return
	}
	index:=r.RaftLog.LastIndex()
	logTerm,_:=r.RaftLog.Term(index)
	msg :=pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: index,
		LogTerm: logTerm,
	}
	r.msgs = append(r.msgs,msg)
}
// sendHeartbeatResponse
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool){
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To:to,
		Term: r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}


// MessageType_MsgHup is a local message used for election. If an election timeout happened
// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
func (r *Raft) handleMsgHup(){
	// Transfer to candidate
	r.becomeCandidate()
	// Send RequestVote RPCs to all others servers
	for to := range r.Prs{
		if to != r.id{
			r.sendRequestVote(to)
		}
	}
}
// MessageType_MsgPropose is a local message that proposes to append data to the leader's log entries.
func (r *Raft) handleMsgPropose(m pb.Message){
	// append m.Entries to leader's log
	r.appendEntries(m)
}
// MessageType_MsgBeat is a local message that signals the leader to send a heartbeat
func (r *Raft) handleMsgBeat(){
	for to := range r.Prs{
		if to != r.id{
			msg:=pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				From: r.id,
				To: to,
				Term: r.Term,
			}
			r.msgs = append(r.msgs,msg)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// leader or follower
	if m.Term < r.Term{
		r.sendAppendResponse(m.From,None,None,true)
		return
	}
	r.resetRandomizedElectionTimeout()
	r.electionElapsed = 0
	r.Lead = m.From
	l := r.RaftLog
	lastIndex := l.LastIndex()
	if m.Index > lastIndex{
		//  tell the leader now we need entry with index == lastIndex + 1
		r.sendAppendResponse(m.From,None,lastIndex+1,true)
		return
	}
	if m.Index >= l.FirstIndex(){
		logTerm, err := l.Term(m.Index)
		if err != nil{
			log.Panic(err)
		}
		// local entry [index = m.Index] [term = logTerm]
		// remote entry [index = m.Index] [term = m.LogTerm]
		// logTerm != m.LogTerm ---> return the first entry  [term = logTerm]
		if logTerm != m.LogTerm{
			// when rejecting an AppendEntries request, the follower
			// can include the term of the conflicting entry and the first
			// index it stores for that term
			// l.toSliceIndex(m.Index)+1 confirm as least one entry exists
			index:=l.toLogIndex(uint64(sort.Search(int(l.toSliceIndex(m.Index)+1), func(i int) bool {
				return l.entries[i].Term == logTerm
			})))
			r.sendAppendResponse(m.From,logTerm,index,true)
			return
		}
	}
	// m.Index ∈ [l.first,l.last] and m.Term == l.term(m.Index)
	for i,entry := range m.Entries{
		if entry.Index < l.FirstIndex(){
			continue
		}
		if entry.Index <=l.LastIndex(){
			logTerm,err := l.Term(entry.Index)
			if err != nil{
				log.Panic(err)
			}
			if logTerm != entry.Term{
				// replace
				si := l.toSliceIndex(entry.Index)
				l.entries[si] = *entry
				// truncate
				l.entries = l.entries[:si+1]
				// the new append entry truncate the log entries ,new one is unstable
				l.stabled = min(l.stabled,entry.Index-1)
			}
		}else{
			// directly append
			n := len(m.Entries)
			for j:=i ; j<n; j++{
				l.entries = append(l.entries,*m.Entries[j])
			}
			break
		}
	}
	if m.Commit > l.committed{
		l.committed = min(m.Commit,m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From,None,l.LastIndex(),false)
}
// handleAppendEntriesResponse handle the response of AppendEntriesRPC request
func (r *Raft) handleAppendEntriesResponse(m pb.Message){
	if m.Term < r.Term && m.Term != None{
		return
	}
	// follower reject the AppendEntries RPC request
	// because of prevLogIndex & preTerm do not match
	if m.Reject {
		index := m.Index
		if index == None{
			return
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match{
		r.Prs[m.From].Next = m.Index +1
		r.Prs[m.From].Match = m.Index
		r.leaderTryCommit()
	}
}
// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term{
		r.sendHeartbeatResponse(m.From,true)
		return
	}
	r.becomeFollower(m.Term,m.From)
	r.sendHeartbeatResponse(m.From,false)
}
func (r *Raft) handleHeartbeatResponse(m pb.Message){
	r.sendAppend(m.From)
}
func (r *Raft) handleVoteRequest(m pb.Message){
	mIndex:= r.RaftLog.LastIndex()
	mLogTerm,_:= r.RaftLog.Term(mIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: true,
		Index: mIndex,
		LogTerm: mLogTerm,
	}
	if m.Term < r.Term{
		// ignore
	} else if m.Term == r.Term{
		// every term only vote for one
		// first-come-first-served
		if r.Vote == None || r.Vote == m.From{
			// voter denies its vote if its own log is more up-to-date
			if m.LogTerm > mLogTerm || (m.LogTerm == mLogTerm && m.Index >= mIndex){
				msg.Reject = false
				r.Vote = m.From
			}
		}
	}else if m.Term > r.Term{
		// a new term
		// vote for m.From
		msg.Reject = false
		r.becomeFollower(m.Term,m.From)
		r.Vote = m.From
	}
	msg.Term = r.Term
	r.msgs = append( r.msgs,msg)
}
func (r *Raft) handleVoteResponse(m pb.Message){
	if m.Term != r.Term  {
		return
	}
	r.votes[m.From] = !m.Reject
	pass:=0
	votes:=len(r.votes)
	threshold := len(r.Prs)/2
	for _,p := range r.votes{
		if p{
			pass++
		}
	}
	if pass > threshold{
		r.becomeLeader()
	}else if votes - pass > threshold{
		r.becomeFollower(r.Term,None)
	}
}
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed ++
		if r.heartbeatElapsed >= r.heartbeatTimeout{
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From: r.id,
				To:r.id,
				Term: r.Term,
			})
		}
	case StateFollower,StateCandidate:
		r.electionElapsed ++
		// If a follower receives no communication over a period of time
		// called the election timeout, then it assumes there is no viable leader and begins an election
		if r.electionElapsed >= r.randomizedElectionTimout{
			r.electionElapsed = 0
			// Raft uses randomized election timeouts to ensure that
			// split votes are rare and that they are resolved quickly
			r.resetRandomizedElectionTimeout()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From: r.id,
				To:r.id,
				Term: r.Term,
			})
		}
	}
}
// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}
// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
func (r * Raft) resetRandomizedElectionTimeout(){
	// baseline : r.electionTimout
	// randomizedElectionTimeout : [ x , 2*x-1 ]
	r.randomizedElectionTimout = r.electionTimeout + GlobalRandInt(r.electionTimeout)
}
// appendEntries append entries to leader's log from client
func (r *Raft) appendEntries(m pb.Message){
	lastIndex := r.RaftLog.LastIndex()
	for i,entry := range m.Entries{
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) +1
		if entry.EntryType == pb.EntryType_EntryConfChange{
			// TODO
		}
		r.RaftLog.entries = append(r.RaftLog.entries,*entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex()+1
	r.broadcastAppend()
	// only one node
	if len(r.Prs) == 1{
		// update committed
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}
func (r *Raft) leaderTryCommit(){
	match := make(uint64Slice,len(r.Prs))
	i :=0
	for _, prs := range r.Prs{
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	// 0 [1] 2
	// 0 [1] 2 3
	// 0 1 [2] 3 4
	// 0 1 [2] 3 4 5
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed{
		// try update committed
		logTerm,err := r.RaftLog.Term(n)
		if err != nil{
			log.Panic(err)
		}
		// Raft never commits log entries from previous terms by counting replicas.
		if logTerm == r.Term{
			r.RaftLog.committed = n
			r.broadcastAppend()
		}
	}
}
func (r *Raft) appendNoopEntry(){
	r.RaftLog.simpleAppendEntry(r.Term,nil)
}
func (r *Raft) softState() *SoftState{
	return &SoftState{Lead: r.Lead,RaftState: r.State}
}
func (r *Raft) hardState() pb.HardState{
	return pb.HardState{
		Term: r.Term,
		Vote: r.Vote,
		Commit: r.RaftLog.committed,
	}
}