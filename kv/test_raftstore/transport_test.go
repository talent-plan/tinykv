package test_raftstore

import "testing"

func testPartitionWrite(cluster Cluster) {
	err := cluster.Start()
	if err != nil {
		panic(err)
	}

	key := []byte("k1")
	value := []byte("v1")
	cluster.MustPut(key, value)
	cluster.MustGetEqual(1, key, value)
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(&pdClient)
	cluster := NewCluster(5, &pdClient, &simulator)
	testPartitionWrite(cluster)
}
