package message

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type MsgType int64

const (
	// just a placeholder
	MsgTypeNull MsgType = 0
	// message to start the ticker of peer
	MsgTypeStart MsgType = 1
	// message of base tick to drive the ticker
	MsgTypeTick MsgType = 2
	// message wraps a raft message that should be forwardded to Raft module
	// the raft message is from peer on other store
	MsgTypeRaftMessage MsgType = 3
	// message warps a raft command that maybe a read/write request or admin request
	// the raft command should be proposed to Raft module
	MsgTypeRaftCmd MsgType = 4
	// message to trigger split region
	// it first asks Scheduler for allocating new split region's ids, then schedules a
	// MsyTypeRaftCmd with split admin command
	MsgTypeSplitRegion MsgType = 5
	// message to update region approximate size
	// it is sent by split checker
	MsgTypeRegionApproximateSize MsgType = 6
	// message to trigger gc generated snapshots
	MsgTypeGcSnap MsgType = 7

	// message wraps a raft message to the peer not existing on the Store.
	// It is due to region split or add peer conf change
	MsgTypeStoreRaftMessage MsgType = 101
	// message of store base tick to drive the store ticker, including store heartbeat
	MsgTypeStoreTick MsgType = 106
	// message to start the ticker of store
	MsgTypeStoreStart MsgType = 107
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
	SplitKey    []byte
	Callback    *Callback
}
