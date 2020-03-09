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

// TODO: Delete Start
// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// TODO: Delete End

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

// TODO: Delete method
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

// TODO: Delete method
func newReady(r *Raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.RaftLog.unstableEntries(),
		CommittedEntries: r.RaftLog.nextEnts(),
	}
	if len(r.msgs) != 0 {
		rd.Messages = r.msgs
		r.msgs = nil
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

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Code Here 2C
	// TODO: Delete Start
	prevSoftSt *SoftState
	prevHardSt pb.HardState
	// TODO: Delete End
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here 2C
	// TODO: Delete Start
	if config.ID == 0 {
		panic("config.ID must not be zero")
	}
	r := newRaft(config)
	rn := &RawNode{
		Raft:       r,
		prevSoftSt: r.softState(),
		prevHardSt: r.hardState(),
	}
	return rn, nil
	// TODO: Delete End
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	// Your Code Here 2C
	// TODO: Delete Start
	rn.Raft.tick()
	// TODO: Delete End
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	// Your Code Here 2C
	// TODO: Delete Start
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
	// TODO: Delete End
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	// Your Code Here 2C
	// TODO: Delete Start
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
	// TODO: Delete End
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	// Your Code Here 3A
	// TODO: Delete Start
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
	// TODO: Delete End
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	// Your Code Here 3A
	// TODO: Delete Start
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
	// TODO: Delete End
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// Your Code Here 2C
	// TODO: Delete Start
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.getProgress(m.From); pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
	// TODO: Delete End
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here 2C
	// TODO: Delete Start
	rd := newReady(rn.Raft, rn.prevSoftSt, rn.prevHardSt)
	rn.Raft.msgs = nil
	return rd
	// TODO: Delete End
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here 2C
	// TODO: Delete Start
	r := rn.Raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if snap := rn.GetSnap(); snap != nil && !IsEmptySnap(snap) {
		return true
	}
	if len(r.msgs) > 0 || len(r.RaftLog.unstableEntries()) > 0 || r.RaftLog.hasNextEnts() {
		return true
	}
	return false
	// TODO: Delete End
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here 2C
	// TODO: Delete Start
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	if rn.prevHardSt.Commit != 0 {
		rn.Raft.RaftLog.appliedTo(rn.prevHardSt.Commit)
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		rn.Raft.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
	// TODO: Delete End
}

// TODO: Delete method
// GetProgress return the the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TODO: Delete method
func (rn *RawNode) GetSnap() *pb.Snapshot {
	return rn.Raft.GetSnap()
}

// TODO: Delete method
// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
