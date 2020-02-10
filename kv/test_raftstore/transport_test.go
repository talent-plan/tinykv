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
	cluster.MustGetEqual(1, key, value)
	cluster.Shutdown()
}

func TestNodePartitionWrite(t *testing.T) {
	pdClient := NewMockPDClient(0)
	simulator := NewNodeSimulator(&pdClient)
	cluster := NewCluster(5, &pdClient, &simulator)
	testPartitionWrite(cluster)
}
