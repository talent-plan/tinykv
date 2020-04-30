package test_raftstore

import (
	"math/rand"

	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

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
	for _, storeID := range f.s1 {
		if msg.FromPeer.StoreId == storeID || msg.ToPeer.StoreId == storeID {
			inS1 = true
			break
		}
	}
	for _, storeID := range f.s2 {
		if msg.FromPeer.StoreId == storeID || msg.ToPeer.StoreId == storeID {
			inS2 = true
			break
		}
	}
	return !(inS1 && inS2)
}

func (f *PartitionFilter) After() {}

type DropFilter struct{}

func (f *DropFilter) Before(msg *rspb.RaftMessage) bool {
	return (rand.Int() % 1000) > 100
}

func (f *DropFilter) After() {}
