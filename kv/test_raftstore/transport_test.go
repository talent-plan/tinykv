package test_raftstore

import (
	"log"
	"testing"
)

func testPartitionWrite(cluster Cluster) {
	err := cluster.Start()
	if err != nil {
		log.Panic(err)
	}

	key := []byte("k1")
	value := []byte("v1")
	cluster.MustPut(key, value)
	cluster.MustGetEqual(1, key, value)
	cluster.Shutdown()
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(&pdClient)
	cluster := NewCluster(5, &pdClient, &simulator)
	testPartitionWrite(cluster)
}
