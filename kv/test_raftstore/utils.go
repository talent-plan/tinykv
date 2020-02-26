package test_raftstore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/log"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func SleepMS(ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func NewPeer(storeID, peerID uint64) *metapb.Peer {
	peer := &metapb.Peer{
		StoreId: storeID,
		Id:      peerID,
	}
	return peer
}

func NewBaseRequest(regionID uint64, epoch *metapb.RegionEpoch) raft_cmdpb.RaftCmdRequest {
	req := raft_cmdpb.RaftCmdRequest{}
	req.Header = &raft_cmdpb.RaftRequestHeader{RegionId: regionID, RegionEpoch: epoch}
	return req
}

func NewRequest(regionID uint64, epoch *metapb.RegionEpoch, requests []*raft_cmdpb.Request) raft_cmdpb.RaftCmdRequest {
	req := NewBaseRequest(regionID, epoch)
	req.Requests = requests
	return req
}

func NewAdminRequest(regionID uint64, epoch *metapb.RegionEpoch, request *raft_cmdpb.AdminRequest) *raft_cmdpb.RaftCmdRequest {
	req := NewBaseRequest(regionID, epoch)
	req.AdminRequest = request
	return &req
}

func NewPutCfCmd(cf string, key, value []byte) *raft_cmdpb.Request {
	cmd := &raft_cmdpb.Request{}
	cmd.CmdType = raft_cmdpb.CmdType_Put
	cmd.Put = &raft_cmdpb.PutRequest{Key: key, Value: value, Cf: cf}
	return cmd
}

func NewGetCfCmd(cf string, key []byte) *raft_cmdpb.Request {
	get := &raft_cmdpb.GetRequest{
		Cf:  cf,
		Key: key,
	}
	cmd := &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Get,
		Get:     get,
	}
	return cmd
}

func NewDeleteCfCmd(cf string, key []byte) *raft_cmdpb.Request {
	delete := &raft_cmdpb.DeleteRequest{
		Cf:  cf,
		Key: key,
	}
	cmd := &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Delete,
		Delete:  delete,
	}
	return cmd
}

func NewSnapCmd() *raft_cmdpb.Request {
	cmd := &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap:    &raft_cmdpb.SnapRequest{},
	}
	return cmd
}

func NewTransferLeaderCmd(peer *metapb.Peer) *raft_cmdpb.AdminRequest {
	transferLeader := raft_cmdpb.TransferLeaderRequest{Peer: peer}
	cmd := &raft_cmdpb.AdminRequest{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: &transferLeader,
	}
	return cmd
}

func NewStatusRequest(regionID uint64, peer *metapb.Peer, request *raft_cmdpb.StatusRequest) *raft_cmdpb.RaftCmdRequest {
	req := NewBaseRequest(regionID, &metapb.RegionEpoch{})
	req.Header.Peer = peer
	req.StatusRequest = request
	return &req
}

func NewRegionLeaderCmd() *raft_cmdpb.StatusRequest {
	cmd := raft_cmdpb.StatusRequest{}
	cmd.CmdType = raft_cmdpb.StatusCmdType_RegionLeader
	return &cmd
}

func MustGetCf(engine *engine_util.Engines, cf string, key []byte, value []byte) {
	for i := 0; i < 300; i++ {
		val, err := engine_util.GetCF(engine.Kv, cf, key)
		if err == nil && (value == nil || bytes.Compare(val, value) == 0) {
			return
		}
		SleepMS(20)
	}
	panic(fmt.Sprintf("can't get value %s for key %s", hex.EncodeToString(value), hex.EncodeToString(key)))
}

func MustGetCfEqual(engine *engine_util.Engines, cf string, key []byte, value []byte) {
	MustGetCf(engine, cf, key, value)
}

func MustGetEqual(engine *engine_util.Engines, key []byte, value []byte) {
	MustGetCf(engine, engine_util.CfDefault, key, value)
}

func MustGetCfNone(engine *engine_util.Engines, cf string, key []byte) {
	var val []byte
	var err error
	for i := 0; i < 300; i++ {
		val, err = engine_util.GetCF(engine.Kv, cf, key)
		if err == badger.ErrKeyNotFound {
			return
		}
		SleepMS(20)
	}
	panic(fmt.Sprintf("get value %s for key %s", hex.EncodeToString(val), hex.EncodeToString(key)))
}

func MustGetNone(engine *engine_util.Engines, key []byte) {
	MustGetCfNone(engine, engine_util.CfDefault, key)
}

func NewTestCluster(count int, cfg *config.Config) *Cluster {
	log.SetLevelByString(cfg.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	pdClient := NewMockPDClient(0, uint64(count)+1)
	simulator := NewNodeSimulator(pdClient)
	return NewCluster(count, pdClient, simulator, cfg)
}
