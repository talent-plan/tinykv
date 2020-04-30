package raftstore

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStore(t *testing.T) {
	engines := util.NewTestEngines()
	defer engines.Destroy()
	require.Nil(t, BootstrapStore(engines, 1, 1))
	require.NotNil(t, BootstrapStore(engines, 1, 1))
	_, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	region := new(metapb.Region)
	require.Nil(t, engine_util.GetMeta(engines.Kv, meta.PrepareBootstrapKey, region))
	_, err = meta.GetRegionLocalState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = meta.GetApplyState(engines.Kv, 1)
	require.Nil(t, err)
	_, err = meta.GetRaftLocalState(engines.Raft, 1)
	require.Nil(t, err)

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isRangeEmpty(engines.Kv, meta.RegionMetaPrefixKey(1), meta.RegionMetaPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)

	empty, err = isRangeEmpty(engines.Kv, meta.RegionRaftPrefixKey(1), meta.RegionRaftPrefixKey(2))
	require.Nil(t, err)
	require.True(t, empty)
}
