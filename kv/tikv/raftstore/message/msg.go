package message

import (
	"sync"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
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

	MsgTypeStoreRaftMessage MsgType = 101
	MsgTypeStoreTick        MsgType = 106
	MsgTypeStoreStart       MsgType = 107

	MsgTypeFsmNormal  MsgType = 201
	MsgTypeFsmControl MsgType = 202

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

type Callback struct {
	Resp       *raft_cmdpb.RaftCmdResponse
	RegionSnap *RegionSnapshot // used for GetSnap
	Wg         sync.WaitGroup
}

type RegionSnapshot struct {
	Region metapb.Region
	Txn    *badger.Txn
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.Resp = resp
		cb.Wg.Done()
	}
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	if cb != nil {
		cb.Wg.Wait()
		return cb.Resp
	}
	return nil
}

func NewCallback() *Callback {
	cb := &Callback{}
	cb.Wg.Add(1)
	return cb
}
