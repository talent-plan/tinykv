package raftstore

import (
	"fmt"
	"time"

	"github.com/ngaut/log"
)

const (
	KB          uint64 = 1024
	MB          uint64 = 1024 * 1024
	SplitSizeMb uint64 = 96
)

type Config struct {
	RaftdbPath string

	SnapPath string

	// store capacity. 0 means no limit.
	Capacity uint64

	// raft_base_tick_interval is a base tick interval (ms).
	RaftBaseTickInterval        time.Duration
	RaftHeartbeatTicks          int
	RaftElectionTimeoutTicks    int
	RaftMinElectionTimeoutTicks int
	RaftMaxElectionTimeoutTicks int
	RaftMaxSizePerMsg           uint64
	RaftMaxInflightMsgs         int

	// When the entry exceed the max size, reject to propose it.
	RaftEntryMaxSize uint64

	// Interval to gc unnecessary raft log (ms).
	RaftLogGCTickInterval time.Duration
	// A threshold to gc stale raft log, must >= 1.
	RaftLogGcThreshold uint64
	// When entry count exceed this value, gc will be forced trigger.
	RaftLogGcCountLimit uint64
	// When the approximate size of raft log entries exceed this value,
	// gc will be forced trigger.
	RaftLogGcSizeLimit uint64
	// When a peer is not responding for this time, leader will not keep entry cache for it.
	RaftEntryCacheLifeTime time.Duration
	// When a peer is newly added, reject transferring leader to the peer for a while.
	RaftRejectTransferLeaderDuration time.Duration

	// Interval (ms) to check region whether need to be split or not.
	SplitRegionCheckTickInterval time.Duration
	/// When size change of region exceed the diff since last check, it
	/// will be checked again whether it should be split.
	RegionSplitCheckDiff uint64
	// delay time before deleting a stale peer
	CleanStalePeerDelay          time.Duration
	PdHeartbeatTickInterval      time.Duration
	PdStoreHeartbeatTickInterval time.Duration
	SnapMgrGcTickInterval        time.Duration
	SnapGcTimeout                time.Duration

	NotifyCapacity  uint64
	MessagesPerTick uint64

	/// When a peer is not active for max_peer_down_duration,
	/// the peer is considered to be down and is reported to PD.
	MaxPeerDownDuration time.Duration

	/// If the leader of a peer is missing for longer than max_leader_missing_duration,
	/// the peer would ask pd to confirm whether it is valid in any region.
	/// If the peer is stale and is not valid in any region, it will destroy itself.
	MaxLeaderMissingDuration time.Duration
	/// Similar to the max_leader_missing_duration, instead it will log warnings and
	/// try to alert monitoring systems, if there is any.
	AbnormalLeaderMissingDuration time.Duration
	PeerStaleStateCheckInterval   time.Duration

	LeaderTransferMaxLogLag uint64

	SnapApplyBatchSize uint64

	// The lease provided by a successfully proposed and applied entry.
	RaftStoreMaxLeaderLease time.Duration

	// Right region derive origin region id when split.
	RightDeriveWhenSplit bool

	AllowRemoveLeader bool

	ApplyMaxBatchSize uint64
	ApplyPoolSize     uint64

	StoreMaxBatchSize uint64
	RaftWorkerCnt     int

	ConcurrentSendSnapLimit uint64
	ConcurrentRecvSnapLimit uint64

	GrpcInitialWindowSize uint64
	GrpcKeepAliveTime     time.Duration
	GrpcKeepAliveTimeout  time.Duration
	GrpcRaftConnNum       uint64

	Addr          string
	AdvertiseAddr string
	Labels        []StoreLabel

	SplitCheck *splitCheckConfig
}

type splitCheckConfig struct {
	// For once split check, there are several splitKey produced for batch.
	// batchSplitLimit limits the number of produced split-key for one batch.
	batchSplitLimit uint64

	// When region [a,e) size meets regionMaxSize, it will be split into
	// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
	// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
	regionMaxSize   uint64
	regionSplitSize uint64
}

type StoreLabel struct {
	LabelKey, LabelValue string
}

func NewDefaultConfig() *Config {
	splitSize := SplitSizeMb * MB
	return &Config{
		RaftdbPath:                  "",
		SnapPath:                    "snap",
		Capacity:                    0,
		RaftBaseTickInterval:        1 * time.Second,
		RaftHeartbeatTicks:          2,
		RaftElectionTimeoutTicks:    10,
		RaftMinElectionTimeoutTicks: 0,
		RaftMaxElectionTimeoutTicks: 0,
		RaftMaxSizePerMsg:           1 * MB,
		RaftMaxInflightMsgs:         256,
		RaftEntryMaxSize:            8 * MB,
		RaftLogGCTickInterval:       10 * time.Second,
		RaftLogGcThreshold:          50,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:              splitSize * 3 / 4 / KB,
		RaftLogGcSizeLimit:               splitSize * 3 / 4,
		RaftEntryCacheLifeTime:           30 * time.Second,
		RaftRejectTransferLeaderDuration: 3 * time.Second,
		SplitRegionCheckTickInterval:     10 * time.Second,
		RegionSplitCheckDiff:             splitSize / 8,
		CleanStalePeerDelay:              10 * time.Minute,
		PdHeartbeatTickInterval:          20 * time.Second,
		PdStoreHeartbeatTickInterval:     10 * time.Second,
		NotifyCapacity:                   40960,
		SnapMgrGcTickInterval:            1 * time.Minute,
		SnapGcTimeout:                    4 * time.Hour,
		MessagesPerTick:                  4096,
		MaxPeerDownDuration:              5 * time.Minute,
		MaxLeaderMissingDuration:         2 * time.Hour,
		AbnormalLeaderMissingDuration:    10 * time.Minute,
		PeerStaleStateCheckInterval:      5 * time.Minute,
		LeaderTransferMaxLogLag:          10,
		SnapApplyBatchSize:               10 * MB,
		// Disable consistency check by default as it will hurt performance.
		// We should turn on this only in our tests.
		RaftStoreMaxLeaderLease: 9 * time.Second,
		RightDeriveWhenSplit:    true,
		AllowRemoveLeader:       false,
		ApplyMaxBatchSize:       1024,
		ApplyPoolSize:           2,
		StoreMaxBatchSize:       1024,
		RaftWorkerCnt:           2,
		ConcurrentSendSnapLimit: 32,
		ConcurrentRecvSnapLimit: 32,
		GrpcInitialWindowSize:   2 * 1024 * 1024,
		GrpcKeepAliveTime:       3 * time.Second,
		GrpcKeepAliveTimeout:    60 * time.Second,
		GrpcRaftConnNum:         1,
		Addr:                    "127.0.0.1:20160",
		SplitCheck:              newDefaultSplitCheckConfig(),
	}
}

const (
	// Default region split size.
	splitSizeMB uint64 = 96
	// Default region split keys.
	splitKeys uint64 = 960000
	// Default batch split limit.
	batchSplitLimit uint64 = 10
)

func newDefaultSplitCheckConfig() *splitCheckConfig {
	splitSize := splitSizeMB * MB
	return &splitCheckConfig{
		batchSplitLimit: batchSplitLimit,
		regionSplitSize: splitSize,
		regionMaxSize:   splitSize / 2 * 3,
	}
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

	if c.RaftMinElectionTimeoutTicks == 0 {
		c.RaftMinElectionTimeoutTicks = c.RaftElectionTimeoutTicks
	}

	if c.RaftMaxElectionTimeoutTicks == 0 {
		c.RaftMaxElectionTimeoutTicks = c.RaftElectionTimeoutTicks * 2
	}

	if c.RaftMinElectionTimeoutTicks < c.RaftElectionTimeoutTicks ||
		c.RaftMinElectionTimeoutTicks >= c.RaftMaxElectionTimeoutTicks {
		return fmt.Errorf("invalid timeout range [%v, %v) for timeout %v",
			c.RaftMinElectionTimeoutTicks, c.RaftMaxElectionTimeoutTicks, c.RaftElectionTimeoutTicks)
	}

	if c.RaftLogGcThreshold < 1 {
		return fmt.Errorf("raft log gc threshold must >= 1, not %v", c.RaftLogGcThreshold)
	}

	if c.RaftLogGcSizeLimit == 0 {
		return fmt.Errorf("raft log gc size limit should large than 0.")
	}

	electionTimeout := c.RaftBaseTickInterval * time.Duration(c.RaftElectionTimeoutTicks)
	if electionTimeout < c.RaftStoreMaxLeaderLease {
		return fmt.Errorf("election timeout %v ns is less than % v ns", electionTimeout, c.RaftStoreMaxLeaderLease)
	}

	if c.PeerStaleStateCheckInterval < electionTimeout*2 {
		return fmt.Errorf("peer stale state check interval %v ns is less than election timeout x 2 %v ns",
			c.PeerStaleStateCheckInterval, electionTimeout*2)
	}

	if c.LeaderTransferMaxLogLag < 10 {
		return fmt.Errorf("ratstore.leader-transfer-max-log-lag should be >= 10.")
	}

	if c.AbnormalLeaderMissingDuration < c.PeerStaleStateCheckInterval {
		return fmt.Errorf("abnormal leader missing %v ns is less than peer stale state check interval %v ns",
			c.AbnormalLeaderMissingDuration, c.PeerStaleStateCheckInterval)
	}

	if c.MaxLeaderMissingDuration < c.AbnormalLeaderMissingDuration {
		return fmt.Errorf("max leader missing %v ns is less than abnormal leader missing %v ns",
			c.MaxLeaderMissingDuration, c.AbnormalLeaderMissingDuration)
	}

	if c.ApplyPoolSize == 0 {
		return fmt.Errorf("apply-pool-size should be greater than 0")
	}
	if c.ApplyMaxBatchSize == 0 {
		return fmt.Errorf("apply-max-batch-size should be greater than 0")
	}
	if c.RaftWorkerCnt == 0 {
		return fmt.Errorf("store-pool-size should be greater than 0")
	}
	if c.StoreMaxBatchSize == 0 {
		return fmt.Errorf("store-max-batch-size should be greater than 0")
	}
	return nil
}
