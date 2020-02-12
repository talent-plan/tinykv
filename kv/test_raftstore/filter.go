package test_raftstore

import rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

type Filter interface {
	Before(msgs *rspb.RaftMessage) bool
	After()
}

type PartitionFilter struct {
	s1 []uint64
	s2 []uint64
}

func (f *PartitionFilter) Before(msg *rspb.RaftMessage) bool {
	inS1 := false
	inS2 := false
	for _, nodeID := range f.s1 {
		if msg.FromPeer.StoreId == nodeID || msg.ToPeer.StoreId == nodeID {
			inS1 = true
			break
		}
	}
	for _, nodeID := range f.s2 {
		if msg.FromPeer.StoreId == nodeID || msg.ToPeer.StoreId == nodeID {
			inS2 = true
			break
		}
	}
	return !(inS1 && inS2)
}

func (f *PartitionFilter) After() {}
