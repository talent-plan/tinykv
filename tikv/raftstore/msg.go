package raftstore

import (
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/zhangjinpeng1987/raft"
)

type MsgType int64

const (
	MsgTypeNull                   MsgType = 0
	MsgTypeRaftMessage            MsgType = 1
	MsgTypeRaftCmd                MsgType = 2
	MsgTypeSplitRegion            MsgType = 3
	MsgTypeComputeResult          MsgType = 4
	MsgTypeRegionApproximateSize  MsgType = 5
	MsgTypeRegionApproximateKeys  MsgType = 6
	MsgTypeCompactionDeclineBytes MsgType = 7
	MsgTypeHalfSplitRegion        MsgType = 8
	MsgTypeMergeResult            MsgType = 9
	MsgTypeGcSnap                 MsgType = 10
	MsgTypeClearRegionSize        MsgType = 11
	MsgTypeTick                   MsgType = 12
	MsgTypeSignificantMsg         MsgType = 13
	MsgTypeStart                  MsgType = 14
	MsgTypeApplyRes               MsgType = 15
	MsgTypeNoop                   MsgType = 16

	MsgTypeStoreRaftMessage   MsgType = 101
	MsgTypeStoreSnapshotStats MsgType = 102
	// Clear region size and keys for all regions in the range, so we can force them to re-calculate
	// their size later.
	MsgTypeStoreClearRegionSizeInRange MsgType = 104
	MsgTypeStoreCompactedEvent         MsgType = 105
	MsgTypeStoreTick                   MsgType = 106
	MsgTypeStoreStart                  MsgType = 107

	MsgTypeFsmNormal  MsgType = 201
	MsgTypeFsmControl MsgType = 202

	MsgTypeApply             MsgType = 301
	MsgTypeApplyRegistration MsgType = 302
	MsgTypeApplyProposal     MsgType = 303
	MsgTypeApplyCatchUpLogs  MsgType = 304
	MsgTypeApplyLogsUpToDate MsgType = 305
	MsgTypeApplyDestroy      MsgType = 306

	msgDefaultChanSize = 1024
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

type Callback struct {
	resp *raft_cmdpb.RaftCmdResponse
	wg   sync.WaitGroup
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.resp = resp
		cb.wg.Done()
	}
}

func NewCallback() *Callback {
	cb := &Callback{}
	cb.wg.Add(1)
	return cb
}

type PeerTick int

const (
	PeerTickRaft             PeerTick = 0
	PeerTickRaftLogGC        PeerTick = 1
	PeerTickSplitRegionCheck PeerTick = 2
	PeerTickPdHeartbeat      PeerTick = 3
	PeerTickCheckMerge       PeerTick = 4
	PeerTickPeerStaleState   PeerTick = 5
)

type StoreTick int

const (
	StoreTickCompactCheck     StoreTick = 0
	StoreTickPdStoreHeartbeat StoreTick = 1
	StoreTickSnapGC           StoreTick = 2
	StoreTickConsistencyCheck StoreTick = 3
)

type MsgSignificantType int

const (
	MsgSignificantTypeStatus      MsgSignificantType = 1
	MsgSignificantTypeUnreachable MsgSignificantType = 2
)

type MsgSignificant struct {
	Type           MsgSignificantType
	ToPeerID       uint64
	SnapshotStatus raft.SnapshotStatus
}

type MsgRaftCmd struct {
	SendTime time.Time
	Request  *raft_cmdpb.RaftCmdRequest
	Callback *Callback
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	// It's an encoded key.
	// TODO: support meta key.
	SplitKeys [][]byte
	Callback  *Callback
}

type MsgComputeHashResult struct {
	Index uint64
	Hash  []byte
}

type MsgHalfSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	Policy      pdpb.CheckPolicy
}

type MsgMergeResult struct {
	TargetPeer *metapb.Peer
	Stale      bool
}

type SnapKeyWithSending struct {
	SnapKey   SnapKey
	IsSending bool
}

type MsgGCSnap struct {
	Snaps []SnapKeyWithSending
}

type MsgStoreClearRegionSizeInRange struct {
	StartKey []byte
	EndKey   []byte
}

func newApplyMsg(apply *apply) Msg {
	return Msg{Type: MsgTypeApply, Data: apply}
}

func newRegistrationMsg(peer *Peer) Msg {
	return Msg{Type: MsgTypeApplyRegistration, Data: newRegistration(peer)}
}

func newDestroyMsg(regionID uint64) Msg {
	return Msg{Type: MsgTypeApplyDestroy, Data: regionID}
}
