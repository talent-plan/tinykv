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
	// true for high reliability, prevent data loss when power failure.
	SyncLog bool
	// minimizes disruption when a partitioned node rejoins the cluster by using a two phase election.
	Prevote    bool
	RaftdbPath string

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
	/// Interval (ms) to check whether start compaction for a region.
	RegionCompactCheckInterval time.Duration
	// delay time before deleting a stale peer
	CleanStalePeerDelay time.Duration
	/// Number of regions for each time checking.
	RegionCompactCheckStep uint64
	/// Minimum number of tombstones to trigger manual compaction.
	RegionCompactMinTombstones uint64
	/// Minimum percentage of tombstones to trigger manual compaction.
	/// Should between 1 and 100.
	RegionCompactTombstonesPencent uint64
	PdHeartbeatTickInterval        time.Duration
	PdStoreHeartbeatTickInterval   time.Duration
	SnapMgrGcTickInterval          time.Duration
	SnapGcTimeout                  time.Duration

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

	// Interval (ms) to check region whether the data is consistent.
	ConsistencyCheckInterval time.Duration

	ReportRegionFlowInterval time.Duration

	// The lease provided by a successfully proposed and applied entry.
	RaftStoreMaxLeaderLease time.Duration

	// Right region derive origin region id when split.
	RightDeriveWhenSplit bool

	AllowRemoveLeader bool

	/// Max log gap allowed to propose merge.
	MergeMaxLogGap uint64

	/// Interval to re-propose merge.
	MergeCheckTickInterval time.Duration

	UseDeleteRange bool

	ApplyMaxBatchSize uint64
	ApplyPoolSize     uint64

	StoreMaxBatchSize uint64
	StorePoolSize     uint64

	GrpcInitialWindowSize uint64
	GrpcKeepAliveTime     time.Duration
	GrpcKeepAliveTimeout  time.Duration
	GrpcRaftConnNum       uint64
}

func NewDefaultConfig() *Config {
	splitSize := SplitSizeMb * MB
	return &Config{
		SyncLog:                     true,
		Prevote:                     true,
		RaftdbPath:                  "",
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
		RegionSplitCheckDiff:             splitSize / 16,
		CleanStalePeerDelay:              10 * time.Minute,
		RegionCompactCheckInterval:       5 * time.Minute,
		RegionCompactCheckStep:           100,
		RegionCompactMinTombstones:       10000,
		RegionCompactTombstonesPencent:   30,
		PdHeartbeatTickInterval:          1 * time.Minute,
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
		ConsistencyCheckInterval: 0,
		ReportRegionFlowInterval: 1 * time.Minute,
		RaftStoreMaxLeaderLease:  9 * time.Second,
		RightDeriveWhenSplit:     true,
		AllowRemoveLeader:        false,
		MergeMaxLogGap:           10,
		MergeCheckTickInterval:   10 * time.Second,
		UseDeleteRange:           false,
		ApplyMaxBatchSize:        1024,
		ApplyPoolSize:            2,
		StoreMaxBatchSize:        1024,
		StorePoolSize:            2,
		GrpcInitialWindowSize:    2 * 1024 * 1024,
		GrpcKeepAliveTime:        3 * time.Second,
		GrpcKeepAliveTimeout:     60 * time.Second,
		GrpcRaftConnNum:          1,
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

	if c.MergeMaxLogGap >= c.RaftLogGcCountLimit {
		return fmt.Errorf("Merge log gap %v should be less than log gc limit %v", c.MergeMaxLogGap, c.RaftLogGcCountLimit)
	}

	if c.MergeCheckTickInterval == 0 {
		return fmt.Errorf("raftstore.merge-check-tick-interval can't be 0.")
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

	if c.RegionCompactTombstonesPencent < 1 || c.RegionCompactTombstonesPencent > 100 {
		return fmt.Errorf("region-compact-tombstones-percent must between 1 and 100, current vlaue is %v",
			c.RegionCompactTombstonesPencent)
	}

	if c.ApplyPoolSize == 0 {
		return fmt.Errorf("apply-pool-size should be greater than 0")
	}
	if c.ApplyMaxBatchSize == 0 {
		return fmt.Errorf("apply-max-batch-size should be greater than 0")
	}
	if c.StorePoolSize == 0 {
		return fmt.Errorf("store-pool-size should be greater than 0")
	}
	if c.StoreMaxBatchSize == 0 {
		return fmt.Errorf("store-max-batch-size should be greater than 0")
	}
	return nil
}
