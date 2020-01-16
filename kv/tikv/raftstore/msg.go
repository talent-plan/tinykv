package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/raft"
)

type MsgType int64

const (
	MsgTypeNull                  MsgType = 0
	MsgTypeRaftMessage           MsgType = 1
	MsgTypeRaftCmd               MsgType = 2
	MsgTypeSplitRegion           MsgType = 3
	MsgTypeRegionApproximateSize MsgType = 5
	MsgTypeGcSnap                MsgType = 10
	MsgTypeTick                  MsgType = 12
	MsgTypeSignificantMsg        MsgType = 13
	MsgTypeStart                 MsgType = 14
	MsgTypeApplyRes              MsgType = 15
	MsgTypeNoop                  MsgType = 16

	MsgTypeStoreRaftMessage   MsgType = 101
	MsgTypeStoreSnapshotStats MsgType = 102
	MsgTypeStoreTick          MsgType = 106
	MsgTypeStoreStart         MsgType = 107

	MsgTypeFsmNormal  MsgType = 201
	MsgTypeFsmControl MsgType = 202

	MsgTypeApply             MsgType = 301
	MsgTypeApplyRegistration MsgType = 302
	MsgTypeApplyProposal     MsgType = 303
	MsgTypeApplyDestroy      MsgType = 306
	MsgTypeApplySnapshot     MsgType = 307

	MsgTypeBarrier MsgType = 401

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
)

type StoreTick int

const (
	StoreTickPdStoreHeartbeat StoreTick = 1
	StoreTickSnapGC           StoreTick = 2
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
