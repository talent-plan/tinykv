package test_raftstore

import (
	"testing"

	"github.com/ngaut/log"
)

func testPartitionWrite(cluster Cluster) {
	err := cluster.Start()
	if err != nil {
		log.Fatal(err)
	}

	key := []byte("k1")
	value := []byte("v1")
	cluster.MustPut(key, value)
	cluster.MustGetEqual(2, key, value)

	// regionID := cluster.GetRegion(key).GetId()

	// // transfer leader to (1, 1)
	// peer := NewPeer(1, 1)
	// cluster.MustTransferLeader(regionID, &peer)

	cluster.Shutdown()
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(&pdClient)
	cluster := NewCluster(5, &pdClient, &simulator)
	testPartitionWrite(cluster)
}
