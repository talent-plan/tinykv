package test_raftstore

import (
	"testing"
)

func testPartitionWrite(cluster *Cluster) {
	cluster.Start()
	defer cluster.Shutdown()

	key := []byte("k1")
	value := []byte("v1")
	cluster.MustPut(key, value)
	MustGetEqual(cluster.engines[1], key, value)

	regionID := cluster.GetRegion(key).GetId()

	// transfer leader to (1, 1)
	peer := NewPeer(1, 1)
	cluster.MustTransferLeader(regionID, &peer)

	// leader in majority, partition doesn't affect write/read
	cluster.AddFilter(&PartitionFilter{
		s1: []uint64{1, 2, 3},
		s2: []uint64{4, 5},
	})
	cluster.MustGet(key, value)
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(pdClient)
	cluster := NewCluster(5, pdClient, simulator)
	testPartitionWrite(cluster)
}
