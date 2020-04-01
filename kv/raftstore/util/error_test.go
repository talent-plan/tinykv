package util

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreErrToPbError(t *testing.T) {
	regionId := uint64(1)
	notLeader := &ErrNotLeader{RegionId: regionId, Leader: nil}
	pbErr := RaftstoreErrToPbError(notLeader)
	require.NotNil(t, pbErr.NotLeader)
	assert.Equal(t, regionId, pbErr.NotLeader.RegionId)

	regionNotFound := &ErrRegionNotFound{RegionId: regionId}
	pbErr = RaftstoreErrToPbError(regionNotFound)
	require.NotNil(t, pbErr.RegionNotFound)
	assert.Equal(t, regionId, pbErr.RegionNotFound.RegionId)

	region := &metapb.Region{Id: regionId, StartKey: []byte{0}, EndKey: []byte{1}}

	keyNotInRegion := &ErrKeyNotInRegion{Key: []byte{2}, Region: region}
	pbErr = RaftstoreErrToPbError(keyNotInRegion)
	require.NotNil(t, pbErr.KeyNotInRegion)
	assert.Equal(t, []byte{0}, pbErr.KeyNotInRegion.StartKey)
	assert.Equal(t, []byte{1}, pbErr.KeyNotInRegion.EndKey)
	assert.Equal(t, []byte{2}, pbErr.KeyNotInRegion.Key)

	epochNotMatch := &ErrEpochNotMatch{Regions: []*metapb.Region{region}}
	pbErr = RaftstoreErrToPbError(epochNotMatch)
	require.NotNil(t, pbErr.EpochNotMatch)
	assert.Equal(t, []*metapb.Region{region}, pbErr.EpochNotMatch.CurrentRegions)

	staleCommand := &ErrStaleCommand{}
	pbErr = RaftstoreErrToPbError(staleCommand)
	require.NotNil(t, pbErr.StaleCommand)

	requestStoreId, actualStoreId := uint64(1), uint64(2)
	storeNotMatch := &ErrStoreNotMatch{RequestStoreId: requestStoreId, ActualStoreId: actualStoreId}
	pbErr = RaftstoreErrToPbError(storeNotMatch)
	require.NotNil(t, pbErr.StoreNotMatch)
	assert.Equal(t, requestStoreId, pbErr.StoreNotMatch.RequestStoreId)
	assert.Equal(t, actualStoreId, pbErr.StoreNotMatch.ActualStoreId)
}
