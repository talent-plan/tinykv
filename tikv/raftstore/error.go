package raftstore

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type ErrNotLeader struct {
	RegionId uint64
	Leader   *metapb.Peer
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("region %v is not leader", e.RegionId)
}

type ErrRegionNotFound struct {
	RegionId uint64
}

func (e *ErrRegionNotFound) Error() string {
	return fmt.Sprintf("region %v is not found", e.RegionId)
}

type ErrKeyNotInRegion struct {
	Key    []byte
	Region *metapb.Region
}

func (e *ErrKeyNotInRegion) Error() string {
	return fmt.Sprintf("key %v is not in region %v", e.Key, e.Region)
}

type ErrEpochNotMatch struct {
	Message string
	Regions []*metapb.Region
}

func (e *ErrEpochNotMatch) Error() string {
	return fmt.Sprintf("epoch not match, error msg %v, regions %v", e.Message, e.Regions)
}

type ErrServerIsBusy struct {
	Reason    string
	BackoffMs uint64
}

func (e *ErrServerIsBusy) Error() string {
	return fmt.Sprintf("server is busy, reason %v, backoff ms %v", e.Reason, e.BackoffMs)
}

type ErrStaleCommand struct{}

func (e *ErrStaleCommand) Error() string {
	return fmt.Sprintf("stale command")
}

type ErrStoreNotMatch struct {
	RequestStoreId uint64
	ActualStoreId  uint64
}

func (e *ErrStoreNotMatch) Error() string {
	return fmt.Sprintf("store not match, request store id is %v, but actual store id is %v", e.RequestStoreId, e.ActualStoreId)
}

type ErrRaftEntryTooLarge struct {
	RegionId  uint64
	EntrySize uint64
}

func (e *ErrRaftEntryTooLarge) Error() string {
	return fmt.Sprintf("raft entry too large, region_id: %v, len: %v", e.RegionId, e.EntrySize)
}

func RaftstoreErrToPbError(e error) *errorpb.Error {
	ret := new(errorpb.Error)
	switch err := errors.Cause(e).(type) {
	case *ErrNotLeader:
		ret.NotLeader = &errorpb.NotLeader{RegionId: err.RegionId, Leader: err.Leader}
	case *ErrRegionNotFound:
		ret.RegionNotFound = &errorpb.RegionNotFound{RegionId: err.RegionId}
	case *ErrKeyNotInRegion:
		ret.KeyNotInRegion = &errorpb.KeyNotInRegion{Key: err.Key, RegionId: err.Region.Id,
			StartKey: err.Region.StartKey, EndKey: err.Region.EndKey}
	case *ErrEpochNotMatch:
		ret.EpochNotMatch = &errorpb.EpochNotMatch{CurrentRegions: err.Regions}
	case *ErrServerIsBusy:
		ret.ServerIsBusy = &errorpb.ServerIsBusy{Reason: err.Reason, BackoffMs: err.BackoffMs}
	case *ErrStaleCommand:
		ret.StaleCommand = &errorpb.StaleCommand{}
	case *ErrStoreNotMatch:
		ret.StoreNotMatch = &errorpb.StoreNotMatch{RequestStoreId: err.RequestStoreId, ActualStoreId: err.ActualStoreId}
	case *ErrRaftEntryTooLarge:
		ret.RaftEntryTooLarge = &errorpb.RaftEntryTooLarge{RegionId: err.RegionId, EntrySize: err.EntrySize}
	default:
		ret.Message = e.Error()
	}
	return ret
}
