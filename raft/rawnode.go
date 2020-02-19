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

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(&rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if !IsEmptySnap(&rd.Snapshot) {
		if index := rd.Snapshot.Metadata.Index; index > 0 {
			return index
		}
	}
	return 0
}

func newReady(r *Raft, prevSoftSt *SoftState, prevHardSt pb.HardState, sinceIdx *uint64) Ready {
	rd := Ready{
		Entries: r.RaftLog.unstableEntries(),
	}
	if len(r.msgs) != 0 {
		rd.Messages = r.msgs
		r.msgs = nil
	}
	if sinceIdx != nil {
		rd.CommittedEntries = r.RaftLog.nextEntsSince(*sinceIdx)
	} else {
		rd.CommittedEntries = r.RaftLog.nextEnts()
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.RaftLog.pending_snapshot != nil {
		rd.Snapshot = *r.RaftLog.pending_snapshot
	}
	return rd
}

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
type RawNode struct {
	Raft       *Raft
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func (rn *RawNode) newReady() Ready {
	return newReady(rn.Raft, rn.prevSoftSt, rn.prevHardSt, nil)
}

func (rn *RawNode) commitReady(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}

	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	// Note: Not compatible with tikv implementation.
	// if index := rd.appliedCursor(); index > 0 {
	//	rn.Raft.RaftLog.appliedTo(index)
	//}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		rn.Raft.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

func (rn *RawNode) commitApply(applied uint64) {
	rn.Raft.RaftLog.appliedTo(applied)
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	if config.ID == 0 {
		panic("config.ID must not be zero")
	}
	r := newRaft(config)
	rn := &RawNode{
		Raft: r,
	}
	lastIndex, err := config.Storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// If the log is empty, this is a new RawNode (like StartNode); otherwise it's
	// restoring an existing RawNode (like RestartNode).
	// TODO(bdarnell): rethink RawNode initialization and whether the application needs
	// to be able to tell us when it expects the RawNode to exist.
	if lastIndex == 0 {
		r.becomeFollower(1, None)
		peers := config.peers
		ents := make([]pb.Entry, len(peers))
		for i, peer := range peers {
			cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: peer}
			data, err := cc.Marshal()
			if err != nil {
				panic("unexpected marshal error")
			}
			ents[i] = pb.Entry{EntryType: pb.EntryType_EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
		}
		r.RaftLog.append(ents...)
		r.RaftLog.committed = uint64(len(ents))
	}

	// Set the initial hard and soft states after performing all initialization.
	rn.prevSoftSt = r.softState()
	if lastIndex == 0 {
		rn.prevHardSt = pb.HardState{}
	} else {
		rn.prevHardSt = r.hardState()
	}

	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(ctx, data []byte) error {
	ent := pb.Entry{Data: data, Context: ctx}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(ctx []byte, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data, Context: ctx}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: rn.Raft.nodes()}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: rn.Raft.nodes()}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.getProgress(m.From); pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	rd := rn.newReady()
	rn.Raft.msgs = nil
	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
// Checking logic in this method should be consistent with Ready.containsUpdates().
func (rn *RawNode) HasReady() bool {
	r := rn.Raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if r.RaftLog.pending_snapshot != nil && !IsEmptySnap(r.RaftLog.pending_snapshot) {
		return true
	}
	if len(r.msgs) > 0 || len(r.RaftLog.unstableEntries()) > 0 || r.RaftLog.hasNextEnts() {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	rn.commitReady(rd)
}

func (rn *RawNode) AdvanceApply(applied uint64) {
	rn.commitApply(applied)
}

// Status returns the current status of the given group.
func (rn *RawNode) Status() *Status {
	status := getStatus(rn.Raft)
	return &status
}

// StatusWithoutProgress returns a Status without populating the Progress field
// (and returns the Status as a value to avoid forcing it onto the heap). This
// is more performant if the Progress is not required. See WithProgress for an
// allocation-free way to introspect the Progress.
func (rn *RawNode) StatusWithoutProgress() Status {
	return getStatusWithoutProgress(rn.Raft)
}

// WithProgress is a helper to introspect the Progress for this node and its
// peers.
func (rn *RawNode) WithProgress(visitor func(id uint64, pr Progress)) {
	for id, pr := range rn.Raft.Prs {
		pr := *pr
		visitor(id, pr)
	}
}

// ReportSnapshot reports the status of the sent snapshot.
func (rn *RawNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgSnapStatus, From: id, Reject: rej})
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

func (rn *RawNode) GetSnap() *pb.Snapshot {
	return rn.Raft.GetSnap()
}

func (rn *RawNode) ReadySince(appliedIdx uint64) Ready {
	return newReady(rn.Raft, rn.prevSoftSt, rn.prevHardSt, &appliedIdx)
}

func hardStateIsEmpty(hs *pb.HardState) bool {
	return hs.Commit == 0 && hs.Term == 0 && hs.Vote == 0
}

func hardStateEqual(l, r *pb.HardState) bool {
	return l.Commit == r.Commit && l.Term == r.Term && l.Vote == r.Vote
}

func (rn *RawNode) HasReadySince(appliedIdx *uint64) bool {
	if len(rn.Raft.msgs) != 0 || rn.Raft.RaftLog.unstableEntries() != nil {
		return true
	}
	if snap := rn.GetSnap(); snap != nil && !IsEmptySnap(snap) {
		return true
	}
	hasUnappliedEntries := false
	if appliedIdx != nil {
		hasUnappliedEntries = rn.Raft.RaftLog.hasNextEntsSince(*appliedIdx)
	} else {
		hasUnappliedEntries = rn.Raft.RaftLog.hasNextEnts()
	}
	if hasUnappliedEntries {
		return true
	}
	if rn.Raft.softState() != rn.prevSoftSt {
		return true
	}
	hs := rn.Raft.hardState()
	if hardStateIsEmpty(&hs) && !hardStateEqual(&hs, &rn.prevHardSt) {
		return true
	}
	return false
}
