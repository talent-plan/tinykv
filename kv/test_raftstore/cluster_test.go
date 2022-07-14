package test_raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDifferentDBPath(t *testing.T) {
	cfg := config.NewTestConfig()
	cluster := NewTestCluster(5, cfg)
	cluster.Start()
	defer cluster.Shutdown()
	snapPaths := make(map[string]bool)
	for i := 1; i <= 5; i++ {
		path := cluster.simulator.(*NodeSimulator).nodes[uint64(i)].GetDBPath()
		log.Infof("DBPath of Store %v: %v", i, path)
		assert.False(t, snapPaths[path])
		snapPaths[path] = true
	}
}
