package meta

import (
	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
)

func GetRegionLocalState(db *badger.DB, regionId uint64) (*rspb.RegionLocalState, error) {
	regionLocalState := new(rspb.RegionLocalState)
	if err := engine_util.GetMsg(db, RegionStateKey(regionId), regionLocalState); err != nil {
		return regionLocalState, err
	}
	return regionLocalState, nil
}

func GetRaftLocalState(db *badger.DB, regionId uint64) (*rspb.RaftLocalState, error) {
	raftLocalState := new(rspb.RaftLocalState)
	if err := engine_util.GetMsg(db, RaftStateKey(regionId), raftLocalState); err != nil {
		return raftLocalState, err
	}
	return raftLocalState, nil
}

func GetSnapRaftState(db *badger.DB, regionId uint64) (*rspb.RaftLocalState, error) {
	snapRaftState := new(rspb.RaftLocalState)
	if err := engine_util.GetMsg(db, SnapshotRaftStateKey(regionId), snapRaftState); err != nil {
		return nil, err
	}
	return snapRaftState, nil
}

func GetApplyState(db *badger.DB, regionId uint64) (*rspb.RaftApplyState, error) {
	applyState := new(rspb.RaftApplyState)
	if err := engine_util.GetMsg(db, ApplyStateKey(regionId), applyState); err != nil {
		return nil, err
	}
	return applyState, nil
}

func GetRaftEntry(db *badger.DB, regionId, idx uint64) (*eraftpb.Entry, error) {
	entry := new(eraftpb.Entry)
	if err := engine_util.GetMsg(db, RaftLogKey(regionId, idx), entry); err != nil {
		return nil, err
	}
	return entry, nil
}

const (
	// When we create a region peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	RaftInitLogTerm  = 5
	RaftInitLogIndex = 5
)

func InitRaftLocalState(raftEngine *badger.DB, region *metapb.Region) (*rspb.RaftLocalState, error) {
	raftState, err := GetRaftLocalState(raftEngine, region.Id)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	if err == badger.ErrKeyNotFound {
		raftState = new(rspb.RaftLocalState)
		raftState.HardState = new(eraftpb.HardState)
		if len(region.Peers) > 0 {
			// new split region
			raftState.LastIndex = RaftInitLogIndex
			raftState.HardState.Term = RaftInitLogTerm
			raftState.HardState.Commit = RaftInitLogIndex
			err = engine_util.PutMsg(raftEngine, RaftStateKey(region.Id), raftState)
			if err != nil {
				return raftState, err
			}
		}
	}
	return raftState, nil
}

func InitApplyState(kvEngine *badger.DB, region *metapb.Region) (*rspb.RaftApplyState, error) {
	applyState, err := GetApplyState(kvEngine, region.Id)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	if err == badger.ErrKeyNotFound {
		applyState = new(rspb.RaftApplyState)
		applyState.TruncatedState = new(rspb.RaftTruncatedState)
		if len(region.Peers) > 0 {
			applyState.AppliedIndex = RaftInitLogIndex
			applyState.TruncatedState.Index = RaftInitLogIndex
			applyState.TruncatedState.Term = RaftInitLogTerm
		}
		err = engine_util.PutMsg(kvEngine, ApplyStateKey(region.Id), applyState)
		if err != nil {
			return applyState, err
		}
	}
	return applyState, nil
}

func InitLastTerm(raftEngine *badger.DB, region *metapb.Region,
	raftState *rspb.RaftLocalState, applyState *rspb.RaftApplyState) (uint64, error) {
	lastIdx := raftState.LastIndex
	if lastIdx == 0 {
		return 0, nil
	} else if lastIdx == RaftInitLogIndex {
		return RaftInitLogTerm, nil
	} else if lastIdx == applyState.TruncatedState.Index {
		return applyState.TruncatedState.Term, nil
	} else {
		y.Assert(lastIdx > RaftInitLogIndex)
	}
	e, err := GetRaftEntry(raftEngine, region.Id, lastIdx)
	if err != nil {
		return 0, errors.Errorf("[region %s] entry at %d doesn't exist, may lost data.", region, lastIdx)
	}
	return e.Term, nil
}
