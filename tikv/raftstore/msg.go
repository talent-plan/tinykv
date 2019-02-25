package raftstore

import (
	"time"

	"go.etcd.io/etcd/raft"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type MsgType int

const (
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

	MsgTypeStoreRaftMessage       MsgType = 101
	MsgTypeStoreSnapshotStats     MsgType = 102
	MsgTypeStoreValidateSSTResult MsgType = 103
	// Clear region size and keys for all regions in the range, so we can force them to re-calculate
	// their size later.
	MsgTypeStoreClearRegionSizeInRange MsgType = 104
	MsgTypeStoreCompactedEvent         MsgType = 105
	MsgTypeStoreTick                   MsgType = 106
	MsgTypeStoreStart                  MsgType = 107

	MsgTypeFsmNormal  MsgType = 201
	MsgTypeFsmControl MsgType = 202
)

type Callback func(resp *raft_cmdpb.RaftCmdResponse)

type PeerTick int

const (
	PeerTickRaft             PeerTick = 1
	PeerTickRaftLogGC        PeerTick = 2
	PeerTickSplitRegionCheck PeerTick = 3
	PeerTickPdHeartbeat      PeerTick = 4
	PeerTickCheckMerge       PeerTick = 5
	PeerTickPeerStaleState   PeerTick = 6
)

type StoreTick int

const (
	StoreTickCompactCheck     StoreTick = 1
	StoreTickPdStoreHeartbeat StoreTick = 2
	StoreTickSnapGC           StoreTick = 3
	StoreTickCompactLockCF    StoreTick = 4
	StoreTickConsistencyCheck StoreTick = 5
	StoreTickCleanupImportSSI StoreTick = 6
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

type Msg struct {
	Type MsgType
	// Region Messages.
	RegionID              uint64
	RaftMsg               *raft_serverpb.RaftMessage
	RaftCMD               *MsgRaftCmd
	SplitRegion           *MsgSplitRegion
	ComputeHashResult     *MsgComputeHashResult
	RegionApproximateSize uint64
	RegionApproximateKeys uint64
	ComputeDeclinedBytes  uint64
	HalfSplitRegion       *MsgHalfSplitRegion
	MergeResult           *MsgMergeResult
	GCSnap                *MsgGCSnap
	Tick                  PeerTick
	Significant           *MsgSignificant
	ApplyRes              interface{} // TODO: change to real apply result later

	// Store Messages.
	StoreRaftMsg                *raft_serverpb.RaftMessage
	StoreValidateSSTResult      *MsgStoreValidateSSTResult
	StoreClearRegionSizeInRange *MsgStoreClearRegionSizeInRange
	StoreCompactedEvent         *rocksdb.CompactedEvent
	StoreTick                   StoreTick
	StoreStartStore             *metapb.Store

	// fsm Message.
	Fsm fsm
}

type MsgRaftCmd struct {
	SendTime time.Time
	Request  *raft_cmdpb.RaftCmdRequest
	Callback Callback
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	// It's an encoded key.
	// TODO: support meta key.
	SplitKeys [][]byte
	Callback  Callback
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

type MsgStoreValidateSSTResult struct {
	InvalidSSTs []*import_sstpb.SSTMeta
}

type MsgStoreClearRegionSizeInRange struct {
	StartKey []byte
	EndKey   []byte
}
