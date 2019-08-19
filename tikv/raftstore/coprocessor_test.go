package raftstore

import (
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/assert"
)

func newTestRouter() (*router, <-chan Msg) {
	cfg := NewDefaultConfig()
	storeSender, storeFsm := newStoreFsm(cfg)
	router := newRouter(newPeerRouter(1, storeSender, storeFsm))
	return router, storeFsm.receiver
}

func newTestSplitCheckRegion() *metapb.Region {
	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			new(metapb.Peer),
		},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 5,
			Version: 2,
		},
		StartKey: []byte{'t'},
		EndKey:   []byte{'u'},
	}
	return region
}

func runTestSplitCheckTask(runner *splitCheckRunner, autoSplit bool, region *metapb.Region,
	policy pdpb.CheckPolicy) {
	runner.run(task{
		tp: taskTypeSplitCheck,
		data: &splitCheckTask{
			region:    region,
			autoSplit: autoSplit,
			policy:    policy,
		},
	})
}

func mustSplitAt(t *testing.T, rx <-chan Msg, expectRegion *metapb.Region, expectSplitKeys [][]byte) {
	for {
		msg := <-rx
		if msg.Type == MsgTypeRegionApproximateSize || msg.Type == MsgTypeRegionApproximateKeys {
			assert.Equal(t, msg.RegionID, expectRegion.Id)
		} else if msg.Type == MsgTypeSplitRegion {
			assert.Equal(t, msg.RegionID, expectRegion.Id)
			splitRegion := msg.Data.(*MsgSplitRegion)
			assert.Equal(t, splitRegion.RegionEpoch, expectRegion.RegionEpoch)
			assert.Equal(t, splitRegion.SplitKeys, expectSplitKeys)
			break
		} else {
			panic(fmt.Sprintf("expect split check result. but got %v", msg))
		}
	}
}

// Return a host but doesn't contains table split check observer.
func newTestCoprocessorHost(config *splitCheckConfig, router *router) *CoprocessorHost {
	host := &CoprocessorHost{}

	halfSplitCheckObserver := newHalfSplitCheckObserver(config.regionMaxSize)

	sizeSplitCheckObserver := newSizeSplitCheckObserver(config.regionMaxSize, config.regionSplitSize,
		config.batchSplitLimit, router)

	keysSplitCheckObserver := newKeysSplitCheckObserver(config.regionMaxKeys, config.regionSplitKeys,
		config.batchSplitLimit, router)

	host.registry.splitCheckObservers = append(host.registry.splitCheckObservers, halfSplitCheckObserver,
		sizeSplitCheckObserver, keysSplitCheckObserver)
	return host
}

func TestHalfSplitCheck(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := newTestSplitCheckRegion()

	config := newDefaultSplitCheckConfig()
	config.regionMaxSize = BucketNumberLimit

	router, receiver := newTestRouter()
	host := newTestCoprocessorHost(config, router)
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	// so split key will be t0005
	wb := new(WriteBatch)
	for i := 0; i < 11; i++ {
		key := []byte(fmt.Sprintf("t%04d", i))
		wb.Set(key, key)
	}
	wb.WriteToKV(engines.kv)
	runTestSplitCheckTask(runner, false, region, pdpb.CheckPolicy_SCAN)
	splitKeys := []byte("t0005")
	mustSplitAt(t, receiver, region, [][]byte{splitKeys})
	runTestSplitCheckTask(runner, false, region, pdpb.CheckPolicy_APPROXIMATE)
	mustSplitAt(t, receiver, region, [][]byte{splitKeys})
}

func TestCheckerWithSameMaxAndSplitSize(t *testing.T) {
	checker := newSizeSplitChecker(24, 24, 1, pdpb.CheckPolicy_SCAN)
	region := new(metapb.Region)
	ctx := observerContext{region: region}
	for {
		data := splitCheckKeyEntry{key: []byte("txxxx"), valueSize: 4}
		if checker.onKv(&ctx, data) {
			break
		}
	}
	assert.True(t, len(checker.getSplitKeys()) != 0)
}

func TestCheckerWithMaxTwiceBiggerThanSplitSize(t *testing.T) {
	checker := newSizeSplitChecker(20, 10, 1, pdpb.CheckPolicy_SCAN)
	region := new(metapb.Region)
	ctx := observerContext{region: region}
	for i := 0; i < 2; i++ {
		data := splitCheckKeyEntry{key: []byte("txxxx"), valueSize: 5}
		if checker.onKv(&ctx, data) {
			break
		}
	}
	assert.True(t, len(checker.getSplitKeys()) != 0)
}

func TestLastKeyOfRegion(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	writeBatch := new(WriteBatch)
	dataKeys := [2][]byte{}
	padding := []byte("_r00000005")
	for i := 1; i < 3; i++ {
		tableKey := codec.EncodeInt(tablecodec.TablePrefix(), int64(i))
		key := append(tableKey, padding...)
		writeBatch.Set(key, key)
		dataKeys[i-1] = key
	}
	writeBatch.WriteToKV(engines.kv)

	checkCases := func(startId, endId int64, want []byte) {
		startTableKey := codec.EncodeInt(tablecodec.TablePrefix(), startId)
		endTableKey := codec.EncodeInt(tablecodec.TablePrefix(), endId)
		assert.Equal(t, lastKeyOfRegion(engines.kv.db, startTableKey, endTableKey), want)
	}

	// ["", "") => t2_xx
	startId := int64(0)
	endId := int64(math.MaxInt64)
	checkCases(startId, endId, dataKeys[1])
	// ["t0", "t1") => None
	endId = 1
	checkCases(startId, endId, nil)
	startId = int64(1)
	endId = int64(math.MaxInt64)
	// ["t1", "tMax") => t2_xx
	checkCases(startId, endId, dataKeys[1])
	// ["t1", "t2") => t1_xx
	endId = 2
	checkCases(startId, endId, dataKeys[0])
}

func TestTableSplitCheckObserver(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := newTestSplitCheckRegion()

	// only create table split observer and ignore approximate.
	router, receiver := newTestRouter()
	host := &CoprocessorHost{}
	host.registry.splitCheckObservers = append(host.registry.splitCheckObservers, &tableSplitCheckObserver{})
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	generateTablePrefix := func(tableId int64) []byte {
		return codec.EncodeInt(tablecodec.TablePrefix(), tableId)
	}

	checkCases := func(encodedStartKey, encodedEndKey []byte, tableId interface{}) {
		region.StartKey = encodedStartKey
		region.EndKey = encodedEndKey
		runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
		switch tableId.(type) {
		case nil:
			assert.Equal(t, 0, len(receiver))
		case uint64:
			key := generateTablePrefix(int64(tableId.(uint64)))
			msg := <-receiver
			if msg.Type == MsgTypeSplitRegion {
				splitRegion := msg.Data.(*MsgSplitRegion)
				assert.Equal(t, [][]byte{key}, splitRegion.SplitKeys)
			} else {
				panic(fmt.Sprintf("expect %v but got %v", key, msg.Type))
			}
		}

	}

	// arbitrary padding.
	padding := []byte("_r00000005")

	// Put some tables
	// t1_xx, t3_xx
	wb := new(WriteBatch)
	for i := 1; i < 4; i++ {
		if i%2 == 0 {
			// leave some space.
			continue
		}
		key := append(generateTablePrefix(int64(i)), padding...)
		wb.Set(key, key)
	}
	wb.WriteToKV(engines.kv)

	// ["t0", "t2") => t1
	checkCases(generateTablePrefix(int64(0)), generateTablePrefix(int64(2)), uint64(1))
	// ["t1", "tMax") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(math.MaxInt64), uint64(3))
	// ["t1", "t5") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(int64(5)), uint64(3))
	// ["t2", "t4") => t3
	checkCases(generateTablePrefix(int64(2)), generateTablePrefix(int64(4)), uint64(3))

	// Put some data to t3
	wb = new(WriteBatch)
	for i := 1; i < 4; i++ {
		key := append(generateTablePrefix(int64(3)), padding...)
		key = append(key, byte(i))
		wb.Set(key, key)
	}
	wb.WriteToKV(engines.kv)

	// ["t1", "tMax") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(math.MaxInt64), uint64(3))
	// ["t3", "tMax") => skip
	checkCases(generateTablePrefix(int64(3)), generateTablePrefix(math.MaxInt64), nil)
	// ["t3", "t5") => skip
	checkCases(generateTablePrefix(int64(3)), generateTablePrefix(int64(5)), nil)
}
