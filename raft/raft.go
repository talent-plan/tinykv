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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignElection represents a normal (time-based) election
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// learners contains the IDs of all learner nodes (including self if the
	// local node is a learner) in the raft cluster. learners only receives
	// entries from the leader node. It does not vote or promote itself.
	learners []uint64

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

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	skipBcastCommit bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
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

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	readStates []ReadState

	// the log
	RaftLog *RaftLog

	maxMsgSize         uint64
	maxUncommittedSize uint64
	Prs                map[uint64]*Progress
	LearnerPrs         map[uint64]*Progress
	matchBuf           uint64Slice

	State StateType

	// isLearner is true if the local raft node is a learner.
	IsLearner bool

	votes map[uint64]bool

	msgs []pb.Message

	// the leader id
	Lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	PendingConfIndex uint64
	// The last `BeginMembershipChange` entry. Once we make this change we exit the joint state.
	//
	// This is different than `pending_conf_index` since it is more specific, and also exact.
	// While `pending_conf_index` is conservatively set at times to ensure safety in the
	// one-by-one change method, in joint consensus based changes we track the state exactly. The
	// index here **must** only be set when a `BeginMembershipChange` is present at that index.
	//
	// # Caveats
	//
	// It is important that whenever this is set that `pending_conf_index` is also set to the
	// value if it is greater than the existing value.
	//
	// **Use `Raft::set_pending_membership_change()` to change this value.**
	pendingMembershipChange *pb.ConfChange
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize uint64

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool

	skipBcastCommit bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	learners := c.learners
	if len(cs.Nodes) > 0 || len(cs.Learners) > 0 {
		if len(peers) > 0 || len(learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = cs.Nodes
		learners = cs.Learners
	}
	r := &Raft{
		id:                        c.ID,
		Lead:                      None,
		IsLearner:                 false,
		RaftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,
		Prs:                       make(map[uint64]*Progress),
		LearnerPrs:                make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		skipBcastCommit:           c.skipBcastCommit,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}
	for _, p := range learners {
		if _, ok := r.Prs[p]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.LearnerPrs[p] = &Progress{Next: 1, IsLearner: true}
		if r.id == p {
			r.IsLearner = true
		}
	}

	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.lastTerm())
	return r
}

func (r *Raft) PendingReadCount() int {
	return len(r.readOnly.pendingReadIndex)
}

func (r *Raft) ReadyReadCount() int {
	return len(r.readStates)
}

func (r *Raft) GetSnap() *pb.Snapshot {
	return r.RaftLog.unstable.snapshot
}

func (r *Raft) SkipBcastCommit(skip bool) {
	r.skipBcastCommit = skip
}

func (r *Raft) InLease() bool {
	return r.State == StateLeader && r.checkQuorum
}

func (r *Raft) hasLeader() bool { return r.Lead != None }

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

func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

func (r *Raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *Raft) learnerNodes() []uint64 {
	nodes := make([]uint64, 0, len(r.LearnerPrs))
	for id := range r.LearnerPrs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All campaign messages need to have the term set when sending.
			// - MessageType_MsgRequestVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MessageType_MsgRequestVoteResponse: m.Term is the new r.Term if the MessageType_MsgRequestVote was
			//   granted, non-zero for the same reason MessageType_MsgRequestVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MessageType_MsgPropose, MessageType_MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MessageType_MsgReadIndex is also forwarded to leader.
		if m.MsgType != pb.MessageType_MsgPropose && m.MsgType != pb.MessageType_MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) getProgress(id uint64) *Progress {
	if pr, ok := r.Prs[id]; ok {
		return pr
	}

	return r.LearnerPrs[id]
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (r *Raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.getProgress(to)
	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.Entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.RaftLog.firstIndex(), r.RaftLog.committed, sindex, sterm, to, pr)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term

		entries := make([]*pb.Entry, 0, len(ents))
		for i := range ents {
			entries = append(entries, &ents[i])
		}
		m.Entries = entries
		m.Commit = r.RaftLog.committed
		if n := len(m.Entries); n != 0 {
			last := m.Entries[n-1].Index
			pr.optimisticUpdate(last)
		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.getProgress(to).Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

func (r *Raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.Prs {
		f(id, pr)
	}

	for id, pr := range r.LearnerPrs {
		f(id, pr)
	}
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.Prs.
func (r *Raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}

		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *Raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	// Preserving matchBuf across calls is an optimization
	// used to avoid allocating a new slice on each call.
	if cap(r.matchBuf) < len(r.Prs) {
		r.matchBuf = make(uint64Slice, len(r.Prs))
	}
	mis := r.matchBuf[:len(r.Prs)]
	idx := 0
	for _, p := range r.Prs {
		mis[idx] = p.Match
		idx++
	}
	sort.Sort(mis)
	mci := mis[len(mis)-r.quorum()]
	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1, IsLearner: pr.IsLearner}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	})

	r.PendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *Raft) appendEntry(es ...pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	li = r.RaftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MessageType_MsgBeat after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.Lead = lead
	r.State = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *Raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.State = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *Raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.Lead = r.id
	r.State = StateLeader

	// Conservatively set the PendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.PendingConfIndex = r.RaftLog.LastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) campaign(t CampaignType) {
	r.becomeCandidate()
	voteMsg := pb.MessageType_MsgRequestVote
	term := r.Term

	if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, MsgType: voteMsg, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.lastTerm(), Context: ctx})
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *Raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgRequestVote {
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			inLease := r.checkQuorum && r.Lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}

	case m.Term < r.Term:
		if r.checkQuorum && (m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MessageType_MsgRequestVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MessageType_MsgRequestVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MessageType_MsgRequestVotes which will be ignored,
			// but it will not receive MessageType_MsgAppend or MessageType_MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MessageType_MsgAppendResponse" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election.
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			if n := numOfPendingConf(ents); n != 0 && r.RaftLog.committed > r.RaftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)

			r.campaign(campaignElection)
		} else {
			r.logger.Debugf("%x ignoring MessageType_MsgHup because already leader", r.id)
		}

	case pb.MessageType_MsgRequestVote:
		if r.IsLearner {
			// TODO: learner may need to vote, in case of node down when confchange.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			return nil
		}
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.Lead == None)
		// ...and we believe the candidate is up to date.
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			// Only record real votes.
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}

	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *Raft, m pb.Message) error

func stepLeader(r *Raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgCheckQuorum:
		if !r.checkQuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MessageType_MsgPropose", r.id)
		}
		if _, ok := r.Prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for i, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex > r.RaftLog.applied {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
						e.String(), r.PendingConfIndex, r.RaftLog.applied)
					m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				} else {
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
				}
			}
		}

		es := make([]pb.Entry, 0, len(m.Entries))
		for _, e := range m.Entries {
			es = append(es, *e)
		}
		if !r.appendEntry(es...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	case pb.MessageType_MsgReadIndex:
		if r.quorum() > 1 {
			if r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(r.RaftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return nil
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:
				r.readOnly.addRequest(r.RaftLog.committed, m)
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.RaftLog.committed
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.RaftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.RaftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := r.getProgress(m.From)
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		pr.RecentActive = true

		if m.Reject {
			r.logger.Debugf("%x received MessageType_MsgAppend rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.sendAppend(m.From)
			}
		} else {
			if pr.maybeUpdate(m.Index) {

				if r.maybeCommit() {
					if r.ShouldBcastCommit() {
						r.bcastAppend()
					}
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					r.logger.Infof("%x sent MessageType_MsgTimeoutNow to %x after received MessageType_MsgAppendResponse", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		pr.RecentActive = true

		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			return nil
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				r.send(pb.Message{To: req.From, MsgType: pb.MessageType_MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MessageType_MsgSnapStatus:
	case pb.MessageType_MsgUnreachable:
	case pb.MessageType_MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MessageType_MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MessageType_MsgTimeoutNow from %x", r.id, r.Term, r.State, m.From)
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.Lead, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		if r.promotable() {
			r.logger.Infof("%x [term %d] received MessageType_MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MessageType_MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MessageType_MsgReadIndex:
		if r.Lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MessageType_MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		ents = append(ents, *ent)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MessageType_MsgAppend [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true, RejectHint: r.RaftLog.LastIndex()})
	}
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse, Context: m.Context})
}

func (r *Raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
func (r *Raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}

	// Note: in TiKV, the newly added learner is first replicated as follower and
	// later change to learner when apply snapshot.
	//
	// The normal peer can't become learner.
	// if !r.IsLearner {
	// 	for _, id := range s.Metadata.ConfState.Learners {
	// 		if id == r.id {
	// 			r.logger.Errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", r.id, s.Metadata.Index, s.Metadata.Term)
	// 			return false
	// 		}
	// 	}
	// }

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	r.RaftLog.restore(s)
	r.Prs = make(map[uint64]*Progress)
	r.LearnerPrs = make(map[uint64]*Progress)
	r.restoreNode(s.Metadata.ConfState.Nodes, false)
	r.restoreNode(s.Metadata.ConfState.Learners, true)
	return true
}

func (r *Raft) restoreNode(nodes []uint64, isLearner bool) {
	for _, n := range nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if n == r.id {
			match = next - 1
			r.IsLearner = isLearner
		}
		r.setProgress(n, match, next, isLearner)
		r.logger.Infof("%x restored progress of %x [%s]", r.id, n, r.getProgress(n))
	}
}

func (r *Raft) hasPendingConf() bool {
	return r.PendingConfIndex > r.RaftLog.applied || r.pendingMembershipChange != nil
}

func (r *Raft) ShouldBcastCommit() bool {
	return !r.skipBcastCommit || r.hasPendingConf()
}

// promotable indicates whether state machine can be promoted to Leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}

func (r *Raft) addNode(id uint64) {
	r.addNodeOrLearnerNode(id, false)
}

func (r *Raft) addLearner(id uint64) {
	r.addNodeOrLearnerNode(id, true)
}

func (r *Raft) addNodeOrLearnerNode(id uint64, isLearner bool) {
	pr := r.getProgress(id)
	if pr == nil {
		r.setProgress(id, 0, r.RaftLog.LastIndex()+1, isLearner)
	} else {
		if isLearner && !pr.IsLearner {
			// can only change Learner to Voter
			r.logger.Infof("%x ignored addLearner: do not support changing %x from raft peer to learner.", r.id, id)
			return
		}

		if isLearner == pr.IsLearner {
			// Ignore any redundant addNode calls (which can happen because the
			// initial bootstrapping entries are applied twice).
			return
		}

		// change Learner to Voter, use origin Learner progress
		delete(r.LearnerPrs, id)
		pr.IsLearner = false
		r.Prs[id] = pr
	}

	if r.id == id {
		r.IsLearner = isLearner
	}

	// When a node is first added, we should mark it as recently active.
	// Otherwise, CheckQuorum may cause us to step down if it is invoked
	// before the added node has a chance to communicate with us.
	pr = r.getProgress(id)
	pr.RecentActive = true
}

func (r *Raft) removeNode(id uint64) {
	r.delProgress(id)

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.Prs) == 0 && len(r.LearnerPrs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.State == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}

func (r *Raft) setProgress(id, match, next uint64, isLearner bool) {
	if !isLearner {
		delete(r.LearnerPrs, id)
		r.Prs[id] = &Progress{Next: next, Match: match}
		return
	}

	if _, ok := r.Prs[id]; ok {
		panic(fmt.Sprintf("%x unexpected changing from voter to learner for %x", r.id, id))
	}
	r.LearnerPrs[id] = &Progress{Next: next, Match: match, IsLearner: true}
}

func (r *Raft) delProgress(id uint64) {
	delete(r.Prs, id)
	delete(r.LearnerPrs, id)
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from
// the view of the local Raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
func (r *Raft) checkQuorumActive() bool {
	var act int

	r.forEachProgress(func(id uint64, pr *Progress) {
		if id == r.id { // self is always active
			act++
			return
		}

		if pr.RecentActive && !pr.IsLearner {
			act++
		}

		pr.RecentActive = false
	})

	return act >= r.quorum()
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
func (r *Raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(&e))
	}

	if r.uncommittedSize > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *Raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(&e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}
