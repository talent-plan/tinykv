package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func stepFollower(raft *Raft, m pb.Message) {
}

func stepCandidate(raft *Raft, m pb.Message) {
}

func stepLeader(raft *Raft, m pb.Message) {
}