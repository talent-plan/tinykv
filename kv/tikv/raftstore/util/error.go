package util

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/errors"
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
	case *ErrStaleCommand:
		ret.StaleCommand = &errorpb.StaleCommand{}
	case *ErrStoreNotMatch:
		ret.StoreNotMatch = &errorpb.StoreNotMatch{RequestStoreId: err.RequestStoreId, ActualStoreId: err.ActualStoreId}
	default:
		ret.Message = e.Error()
	}
	return ret
}
