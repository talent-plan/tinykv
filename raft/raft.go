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
	"time"

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
	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		Prs:              make(map[uint64]*Progress),
		Vote:             None,
		votes:            make(map[uint64]bool),
		Term:             None,
	}
	for _, peer := range c.peers {
		if peer != c.ID {
			raft.Prs[peer] = &Progress{}
		}

	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var entries []*pb.Entry
	toSend := r.RaftLog.entries[r.Prs[to].Next-1:]
	for _, entry := range toSend {
		entries = append(entries, &entry)
	}

	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Commit:  r.RaftLog.committed,
		Entries: entries,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed += 1
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for peer, _ := range r.Prs {
				r.sendHeartbeat(peer)
			}
			r.heartbeatElapsed = 0
		}
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			rand.Seed(time.Now().UnixMicro())
			r.electionTimeout = rand.Intn(10*r.heartbeatTimeout) + 10*r.heartbeatTimeout
			// r.msgs = append(r.msgs,pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.msgs = nil
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Term += 1
	r.State = StateCandidate
	r.votes[r.id] = true
	// for peer, _ := range r.Prs {
	// 	r.msgs = append(r.msgs, pb.Message{
	// 		MsgType: pb.MessageType_MsgRequestVote,
	// 		From:    r.id,
	// 		To:      peer,
	// 		Term:    r.Term,
	// 	})
	// }
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// change state
	r.State = StateLeader
	r.Lead = r.id
	// recover from storage
	lastIndex, _ := r.RaftLog.storage.LastIndex()
	firstIndex, _ := r.RaftLog.storage.FirstIndex()
	oldEntries, _ := r.RaftLog.storage.Entries(firstIndex, lastIndex+1)
	if len(oldEntries) != 0 {
		r.RaftLog.entries = append(r.RaftLog.entries, oldEntries...)
		r.RaftLog.committed = oldEntries[len(oldEntries)-1].Index
		r.RaftLog.applied = r.RaftLog.committed
		// for peer, _ := range r.Prs {
			// r.sendAppend()
		// }
	}

	// initialize progress
	for peer, _ := range r.Prs {
		r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		r.Prs[peer].Match = 0
	}
	// commit noop entry
	entries := make([]*pb.Entry, 1)
	entries[0] = &pb.Entry{
		Data:  nil,
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, *entries[0])
	// bcast the noop entry to all peers
	prevLogIndex := r.RaftLog.LastIndex()
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	for peer, _ := range r.Prs {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
			Index:   prevLogIndex,
			LogTerm: prevLogTerm,
			Entries: entries,
		})
	}
	// immediately commit if there are no other servers
	if len(r.Prs) == 0 {
		r.RaftLog.committed += 1
	}

	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			// transform to candidate
			r.becomeCandidate()
			r.bcastVoteRequests()
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			// leader heartbeat
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			// append entries
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgPropose {
			// redirect
			if r.Lead != 0 {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			// vote
			var reject bool
			var newTerm uint64
			if (r.Vote == None || r.Vote == m.From) && ((m.Term > r.Term) || ((m.Term == r.Term) && (m.Commit >= r.RaftLog.committed))) {
				reject = false
				r.Vote = m.From
				newTerm = m.Term
			} else {
				reject = true
				newTerm = r.Term
			}
			r.Term = newTerm
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    newTerm,
				Reject:  reject,
			})
		}
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgAppend {
			r.becomeFollower(m.Term, m.From)
		} else if m.MsgType == pb.MessageType_MsgHup {
			r.becomeCandidate()
			r.bcastVoteRequests()
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			if m.Reject == false {
				r.votes[m.From] = true
			}
			r.checkMajority()
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			if r.Term < m.Term {
				r.Term = m.Term
				r.becomeFollower(m.Term, 0)
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject:  false,
					From:    r.id,
					To:      m.From,
					Term:    m.Term,
				})
			} else if r.Term > m.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject:  true,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			}
		}
	case StateLeader:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}
		if m.MsgType == pb.MessageType_MsgPropose {
			for _, entry := range m.Entries {
				entry.Term = r.Term
				entry.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			for peer, _ := range r.Prs {
				r.sendAppend(peer)
			}
			if len(r.Prs) == 0 {
				r.RaftLog.committed += 1
			}
			// } else if m.MsgType == pb.MessageType_MsgAppend {
			// 	for peer, _ := range r.Prs {
			// 		r.sendAppend(peer)
			// 	}
		} else if m.MsgType == pb.MessageType_MsgBeat {
			for peer, _ := range r.Prs {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgHeartbeat,
					From:    r.id,
					To:      peer,
					Term:    r.Term,
					Entries: make([]*pb.Entry, 0),
				})
			}

		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			if m.Term > r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject:  true,
					From:    r.id,
					To:      m.From,
				})
			} else {
				r.becomeFollower(m.Term, 0)
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject:  false,
					From:    r.id,
					To:      m.From,
					Term:    m.Term,
				})
			}
		} else if m.MsgType == pb.MessageType_MsgAppendResponse {
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			if m.Reject {
				r.Prs[m.From].Next -= 1
				r.sendAppend(m.From)
			} else {
				r.Prs[m.From].Next = m.Index + 1
				r.Prs[m.From].Match = m.Index
				r.checkMajorityLog(m)
			}
		}
	}
	return nil
}

// candidate broadcast RequestVote RPCs in parallel
func (r *Raft) bcastVoteRequests() {
	// check before bcast, in case there is no peer
	r.checkMajority()
	for peer, _ := range r.Prs {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
		})
	}
}

// check whether server got majority votes
func (r *Raft) checkMajority() {
	count := 0
	total := 1
	for range r.Prs {
		total += 1
	}
	for _, vote := range r.votes {
		if vote {
			count += 1
		}
	}
	if count >= (total/2 + 1) {
		r.becomeLeader()
	}
}
func (r *Raft) checkMajorityLog(m pb.Message) {
	total := len(r.Prs)
	i := r.RaftLog.committed + 1
	for ; i <= r.RaftLog.LastIndex(); i++ {
		counter := 0
		for _, progress := range r.Prs {
			if progress.Match >= i {
				counter += 1
			}
		}
		if counter >= (total+1)/2 {
			r.RaftLog.committed += 1
		} else {
			break
		}
	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	}
	if m.Term > r.Term {
		if r.State == StateCandidate || r.State == StateLeader {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.Term = m.Term
		}
	}

	term, err := r.RaftLog.Term(m.Index)
	if term != m.LogTerm {
		if err == nil {
			r.RaftLog.entries = r.RaftLog.entries[:m.Index-1]
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
	} else {
		for _, entry := range m.Entries {
			entry.Term = m.Term
			entry.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Reject:  false,
			Index:   r.RaftLog.LastIndex(),
		})

	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateCandidate && r.Term > m.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
		})
	} else {
		r.Lead = m.From
		r.Term = m.Term
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		} else if r.State == StateFollower {
			r.electionElapsed = 0
		}
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
