package raftstore

import (
	"fmt"
	stdatomic "sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/uber-go/atomic"
)

type LeaderChecker interface {
	IsLeader(ctx *kvrpcpb.Context, router RaftstoreRouter) error
}

type leaderChecker struct {
	peerID           uint64
	invalid          atomic.Bool
	term             atomic.Uint64
	appliedIndexTerm atomic.Uint64
	leaderLease      unsafe.Pointer // *RemoteLease
	region           unsafe.Pointer // *metapb.Region
}

func (c *leaderChecker) IsLeader(ctx *kvrpcpb.Context, router RaftstoreRouter) error {
	snapTime := time.Now()
	isExpired, err := c.isExpired(ctx, &snapTime)
	if err != nil {
		return err
	}
	if !isExpired {
		return nil
	}

	cb := NewCallback()
	req := new(raft_cmdpb.Request)
	req.CmdType = raft_cmdpb.CmdType_Snap
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
		SyncLog:     ctx.SyncLog,
	}
	cmd := &raft_cmdpb.RaftCmdRequest{
		Header:   header,
		Requests: []*raft_cmdpb.Request{req},
	}

	err = router.SendCommand(cmd, cb)
	if err != nil {
		return err
	}

	cb.wg.Wait()

	if cb.resp.Header.Error != nil {
		return &RaftError{e: cb.resp.Header.Error}
	}
	return nil
}

func (c *leaderChecker) isExpired(ctx *kvrpcpb.Context, snapTime *time.Time) (bool, error) {
	if c.invalid.Load() {
		return false, &ErrRegionNotFound{RegionId: ctx.RegionId}
	}

	peerID := c.peerID
	term := c.term.Load()
	region := (*metapb.Region)(stdatomic.LoadPointer(&c.region))
	lease := (*RemoteLease)(stdatomic.LoadPointer(&c.leaderLease))
	appliedIndexTerm := c.appliedIndexTerm.Load()

	if ctx.Peer.Id != peerID {
		return false, errors.Errorf("mismatch peer id %d != %d", ctx.Peer.Id, peerID)
	}

	if ctx.Term == 0 || term <= ctx.Term+1 {
		return false, &ErrStaleCommand{}
	}

	if ctx.RegionEpoch == nil {
		return false, errors.New("missing epoch")
	}
	// for get request, we don't care ConfVersion.
	if ctx.RegionEpoch.Version != region.RegionEpoch.Version {
		err := &ErrEpochNotMatch{}
		err.Message = fmt.Sprintf("current epoch of region %d is %s, but you sent %s",
			region.Id, region.RegionEpoch, ctx.RegionEpoch)
		return false, err
	}

	if appliedIndexTerm != term {
		return false, nil
	}
	return lease.Inspect(snapTime) == LeaseState_Valid, nil
}
