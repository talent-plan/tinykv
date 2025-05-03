#!/bin/bash

bench_file='
package test_raftstore

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
)

func TestReadWrite(t *testing.T) {
	nservers := 3
	cfg := config.NewTestConfig()
	cfg.RaftLogGcCountLimit = uint64(100)
	cfg.RegionMaxSize = 300
	cfg.RegionSplitSize = 200
	cluster := NewTestCluster(nservers, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	electionTimeout := cfg.RaftBaseTickInterval * time.Duration(cfg.RaftElectionTimeoutTicks)
	// Wait for leader election
	time.Sleep(2 * electionTimeout)

	nclients := 16
	chTasks := make(chan int, 1000)
	clnts := make([]chan bool, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan bool, 1)
		go func(cli int) {
			defer func() {
				clnts[cli] <- true
			}()
			for {
				j, more := <-chTasks
				if more {
					key := fmt.Sprintf("%02d%08d", cli, j)
					value := "x " + strconv.Itoa(j) + " y"
					cluster.MustPut([]byte(key), []byte(value))
					if (rand.Int() % 1000) < 500 {
						value := cluster.Get([]byte(key))
						if value == nil {
							t.Fatal("value is emtpy")
						}
					}
				} else {
					return
				}
			}
		}(i)
	}

	start := time.Now()
	duration := 3 * time.Minute
	totalRequests := 0
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			if elapsed >= duration {
				close(chTasks)
				for cli := 0; cli < nclients; cli++ {
					ok := <-clnts[cli]
					if !ok {
						t.Fatalf("failure")
					}
				}
				ticker.Stop()
				totalDuration := time.Since(start)
				t.Logf("Total Duration: %v, Total Requests: %v", totalDuration, totalRequests)
				t.Logf("QPS: %v", float64(totalRequests)/totalDuration.Seconds())
				return
			}
		case chTasks <- totalRequests:
			totalRequests++
		}
	}
}
'

echo "$bench_file" > kv/test_raftstore/bench_test.go
go test ./kv/test_raftstore/ -run ReadWrite -v > bench.log
score=$(grep QPS: bench.log | awk '{print $3}')
rm bench.log

if [ -z "$score" ]
then
  score=0
fi

echo $score
