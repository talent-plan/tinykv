package raftstore

import (
	"os"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
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
	require.Nil(t, getMsg(engines.Kv, prepareBootstrapKey, region))
	regionLocalState := new(rspb.RegionLocalState)
	require.Nil(t, getMsg(engines.Kv, RegionStateKey(1), regionLocalState))
	raftApplyState := applyState{}
	val, err := getValue(engines.Kv, ApplyStateKey(1))
	require.Nil(t, err)
	raftApplyState.Unmarshal(val)
	raftLocalState := raftState{}
	val, err = getValue(engines.Raft, RaftStateKey(1))
	require.Nil(t, err)
	raftLocalState.Unmarshal(val)

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isRangeEmpty(engines.Kv, RegionMetaPrefixKey(1), RegionMetaPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)

	empty, err = isRangeEmpty(engines.Kv, RegionRaftPrefixKey(1), RegionRaftPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)
}
