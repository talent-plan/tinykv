package raftstore

import (
	"os"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStore(t *testing.T) {
	engines := newTestEngines(t)
	defer func() {
		os.RemoveAll(engines.kvPath)
		os.RemoveAll(engines.raftPath)
	}()
	require.Nil(t, BootstrapStore(engines, 1, 1))
	require.NotNil(t, BootstrapStore(engines, 1, 1))
	_, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	region := new(metapb.Region)
	require.Nil(t, getMsg(engines.kv, prepareBootstrapKey, region))
	regionLocalState := new(rspb.RegionLocalState)
	require.Nil(t, getMsg(engines.kv, RegionStateKey(1), regionLocalState))
	raftApplyState := new(rspb.RaftApplyState)
	require.Nil(t, getMsg(engines.kv, ApplyStateKey(1), raftApplyState))
	raftLocalState := new(rspb.RaftLocalState)
	require.Nil(t, getMsg(engines.raft, RaftStateKey(1), raftLocalState))

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isRangeEmpty(engines.kv, RegionMetaPrefixKey(1), RegionMetaPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)

	empty, err = isRangeEmpty(engines.kv, RegionRaftPrefixKey(1), RegionRaftPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)
}
