package message

import (
	"time"

	"github.com/coocood/badger"
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

	MsgTypeApply             MsgType = 301
	MsgTypeApplyRegistration MsgType = 302
	MsgTypeApplyProposal     MsgType = 303
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

type Callback struct {
	Resp *raft_cmdpb.RaftCmdResponse
	Txn  *badger.Txn // used for GetSnap
	done chan struct{}
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if resp != nil {
		cb.Resp = resp
	}
	cb.done <- struct{}{}
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	}
}

func (cb *Callback) WaitRespWithTimeout(timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	case <-time.After(timeout):
		return cb.Resp
	}
}

func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}
