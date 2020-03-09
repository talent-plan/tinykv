package message

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type MsgType int64

const (
	MsgTypeNull                  MsgType = 0
	MsgTypeStart                 MsgType = 1
	MsgTypeTick                  MsgType = 2
	MsgTypeRaftMessage           MsgType = 3
	MsgTypeRaftCmd               MsgType = 4
	MsgTypeApplyRes              MsgType = 5
	MsgTypeGcSnap                MsgType = 7
	MsgTypeSplitRegion           MsgType = 8
	MsgTypeRegionApproximateSize MsgType = 9

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

type MsgGCSnap struct {
	Snaps []snap.SnapKeyWithSending
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
