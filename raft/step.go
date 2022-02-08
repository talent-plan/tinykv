package raft

import (
	"fmt"
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func stepFollower(r *Raft, m pb.Message) error {
	// follower's responsibility
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campaignForLeader()
		r.requestVotesFromPeers()
		if enableExtraLog() {
			log.Printf("r%d: msg hup , state:%d \n", r.id, r.State)
		}
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error  {
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
		if enableExtraLog() {
			log.Printf("r.id:%d default message handle %+v \n", r.id, m)
		}
		//	r.Term = m.Term
		//	r.becomeFollower(m.Term, m.From)
		//	r.msgs = append(r.msgs, m)
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
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

		if enableExtraLog() {
			log.Printf("[MsgPropose]: broadcastAppend log entries:%d \n", len(r.RaftLog.entries))
		}

		r.broadcastAppend()
		if enableExtraLog() {
			log.Printf("finished broadcast append")
		}
	case pb.MessageType_MsgAppendResponse:
		prTracker, ok := r.Prs[m.From]
		if !ok {
			return fmt.Errorf("Can't find peer:%d in Prs ", m.From)
		}
		if m.Reject {

		} else {
			// log.Printf("pr try update index:%d", m.Index)
			// log.Printf("recv message: %+v", m)
			prTracker.TryUpdate(m.Index)
			if r.maybeCommit() {
				 r.broadcastAppend()  // 暂时没有搞懂，为什么要 broadcastAppend ?
			}
		}
	}
	return nil
}