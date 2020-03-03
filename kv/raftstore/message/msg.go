package message

import (
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
	MsgTypeSnapStatus            MsgType = 13
	MsgTypeStart                 MsgType = 14
	MsgTypeApplyRes              MsgType = 15

	MsgTypeStoreRaftMessage MsgType = 101
	MsgTypeStoreTick        MsgType = 106
	MsgTypeStoreStart       MsgType = 107

	MsgTypeApplyCommitted MsgType = 301
	MsgTypeApplyRefresh   MsgType = 302
	MsgTypeApplyProposal  MsgType = 303
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

type MsgRaftCmd struct {
	Request  *raft_cmdpb.RaftCmdRequest
	Callback *Callback
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	SplitKeys   [][]byte
	Callback    *Callback
}

type MsgSnapStatus struct {
	ToPeerID       uint64
	SnapshotStatus raft.SnapshotStatus
}
