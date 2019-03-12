package raftstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	cfg := NewDefaultConfig()
	require.Nil(t, cfg.Validate())

	assert.Equal(t, cfg.RaftMinElectionTimeoutTicks, cfg.RaftElectionTimeoutTicks)
	assert.Equal(t, cfg.RaftMaxElectionTimeoutTicks, cfg.RaftElectionTimeoutTicks*2)

	cfg.RaftHeartbeatTicks = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftElectionTimeoutTicks = 10
	cfg.RaftHeartbeatTicks = 10
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftMinElectionTimeoutTicks = 5
	require.NotNil(t, cfg.Validate())
	cfg.RaftMinElectionTimeoutTicks = 25
	require.NotNil(t, cfg.Validate())
	cfg.RaftMinElectionTimeoutTicks = 10
	require.Nil(t, cfg.Validate())

	cfg.RaftHeartbeatTicks = 11
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftLogGcSizeLimit = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftBaseTickInterval = 1 * time.Second
	cfg.RaftElectionTimeoutTicks = 10
	cfg.RaftStoreMaxLeaderLease = 20 * time.Second
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftLogGcCountLimit = 100
	cfg.MergeMaxLogGap = 110
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.MergeCheckTickInterval = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.RaftBaseTickInterval = 1 * time.Second
	cfg.RaftElectionTimeoutTicks = 10
	cfg.PeerStaleStateCheckInterval = 5 * time.Second
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.PeerStaleStateCheckInterval = 2 * time.Minute
	cfg.AbnormalLeaderMissingDuration = 1 * time.Minute
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.AbnormalLeaderMissingDuration = 2 * time.Minute
	cfg.MaxLeaderMissingDuration = 1 * time.Minute
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.LocalReadBatchSize = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.ApplyMaxBatchSize = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.ApplyPoolSize = 0
	require.NotNil(t, cfg.Validate())

	cfg = NewDefaultConfig()
	cfg.FuturePoolSize = 0
	require.NotNil(t, cfg.Validate())
}
