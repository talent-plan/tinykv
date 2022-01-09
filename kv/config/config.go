package config

import (
	"fmt"
	"os"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
)

type Config struct {
	StoreAddr     string
	Raft          bool
	SchedulerAddr string
	LogLevel      string

	DBPath string // Directory to store the data in. Should exist and be writable.

	// raft_base_tick_interval is a base tick interval (ms).
	RaftBaseTickInterval     time.Duration
	RaftHeartbeatTicks       int
	RaftElectionTimeoutTicks int

	// Interval to gc unnecessary raft log (ms).
	RaftLogGCTickInterval time.Duration
	// When entry count exceed this value, gc will be forced trigger.
	RaftLogGcCountLimit uint64

	// Interval (ms) to check region whether need to be split or not.
	SplitRegionCheckTickInterval time.Duration
	// delay time before deleting a stale peer
	SchedulerHeartbeatTickInterval      time.Duration
	SchedulerStoreHeartbeatTickInterval time.Duration

	// When region [a,e) size meets regionMaxSize, it will be split into
	// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
	// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
	RegionMaxSize   uint64
	RegionSplitSize uint64
}

func (c *Config) Validate() error {
	if c.RaftHeartbeatTicks == 0 {
		return fmt.Errorf("heartbeat tick must greater than 0")
	}

	if c.RaftElectionTimeoutTicks != 10 {
		log.Warnf("Election timeout ticks needs to be same across all the cluster, " +
			"otherwise it may lead to inconsistency.")
	}

	if c.RaftElectionTimeoutTicks <= c.RaftHeartbeatTicks {
		return fmt.Errorf("election tick must be greater than heartbeat tick.")
	}

	return nil
}

const (
	KB uint64 = 1024
	MB uint64 = 1024 * 1024
)

func getLogLevel() (logLevel string) {
	logLevel = "info"
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		logLevel = l
	}
	return
}

func NewDefaultConfig() *Config {
	return &Config{
		SchedulerAddr:            "127.0.0.1:2379",
		StoreAddr:                "127.0.0.1:20160",
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     1 * time.Second,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    10 * time.Second,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        10 * time.Second,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 10 * time.Second,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}

func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     50 * time.Millisecond,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    50 * time.Millisecond,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        100 * time.Millisecond,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}
