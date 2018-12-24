package tikv

import (
	"bytes"
	"github.com/dgryski/go-farm"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	otablecodec "github.com/pingcap/tidb/tablecodec/origin"
	"github.com/pingcap/tidb/types"
	"time"
)

func exceedEndKey(current, endKey []byte) bool {
	return bytes.Compare(current, endKey) >= 0
}

func extractPhysicalTime(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.Unix(t/1e3, (t%1e3)*1e6)
}

func tsSub(tsA, tsB uint64) time.Duration {
	tsAPhysical := int64(tsA >> 18)
	tsBPhysical := int64(tsB >> 18)
	return time.Duration(tsAPhysical-tsBPhysical) * time.Millisecond
}

func mutationsToHashVals(mutations []*kvrpcpb.Mutation) []uint64 {
	hashVals := make([]uint64, len(mutations))
	for i, mut := range mutations {
		hashVals[i] = farm.Fingerprint64(mut.Key)
	}
	return hashVals
}

func keysToHashVals(keys ...[]byte) []uint64 {
	hashVals := make([]uint64, len(keys))
	for i, key := range keys {
		hashVals[i] = farm.Fingerprint64(key)
	}
	return hashVals
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func isRowKey(key []byte) bool {
	if IsShardingEnabled() {
		return rowcodec.IsRowKeyWithShardByte(key)
	}
	return rowcodec.IsRowKey(key)
}

func cutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	if IsShardingEnabled() {
		return tablecodec.CutIndexKeyNew(key, length)
	}
	return otablecodec.CutIndexKeyNew(key, length)
}

func decodeRowKey(key kv.Key) (int64, error) {
	if IsShardingEnabled() {
		return tablecodec.DecodeRowKey(key)
	}
	return otablecodec.DecodeRowKey(key)
}

func cutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	if IsShardingEnabled() {
		return tablecodec.CutRowNew(data, colIDs)
	}
	return otablecodec.CutRowNew(data, colIDs)
}

func decodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if IsShardingEnabled() {
		return tablecodec.DecodeColumnValue(data, ft, loc)
	}
	return otablecodec.DecodeColumnValue(data, ft, loc)
}
