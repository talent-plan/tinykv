package raftstore

import (
	"github.com/pingcap/kvproto/pkg/eraftpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type ApplyTask struct {
	RegionId uint64
	Term     uint64
	Entries  []eraftpb.Entry
}

type ApplyMetrics struct {
	SizeDiffHint       uint64
	DeleteKeysHint     uint64
	WrittenBytes       uint64
	WrittenKeys        uint64
	LockCfWrittenBytes uint64
}

type ApplyTaskRes struct {
	regionID         uint64
	applyState       rspb.RaftApplyState
	appliedIndexTerm uint64
	execResults      []execResult
	metrics          *ApplyMetrics
	merged           bool

	destroyPeerID uint64
}

type execResult struct {
	// TODO: place holder
}

type ApplyRouter struct {
	// Todo: currently it is a place holder
}

func (a *ApplyRouter) ScheduleTask(regionId uint64, task *ApplyTask) {
	// Todo: currently it is a place holder
}
