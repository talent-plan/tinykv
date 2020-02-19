package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	builder := newBuilder(t)
	builder.mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})

	var req kvrpcpb.RawGetRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault
	get := NewRawGet(&req)
	resp := builder.runOneCmd(&get).(*kvrpcpb.RawGetResponse)

	assert.Equal(t, []byte{42}, resp.Value)
}

func TestPut(t *testing.T) {
	builder := newBuilder(t)

	var req kvrpcpb.RawPutRequest
	req.Key = []byte{99}
	req.Value = []byte{42}
	req.Cf = engine_util.CfDefault
	put := NewRawPut(&req)
	builder.runCommands(&put)

	assert.Equal(t, 1, builder.mem.Len(engine_util.CfDefault))
	assert.Equal(t, []byte{42}, builder.mem.Get(engine_util.CfDefault, []byte{99}))
}

func TestDelete(t *testing.T) {
	builder := newBuilder(t)
	builder.mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})

	var req kvrpcpb.RawDeleteRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault
	del := NewRawDelete(&req)
	builder.runCommands(&del)

	assert.Equal(t, 0, builder.mem.Len(engine_util.CfDefault))
}

func TestScan(t *testing.T) {
	builder := newBuilder(t)
	builder.mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})
	builder.mem.Set(engine_util.CfDefault, []byte{101}, []byte{42, 2})
	builder.mem.Set(engine_util.CfDefault, []byte{102}, []byte{42, 3})
	builder.mem.Set(engine_util.CfDefault, []byte{105}, []byte{42, 4})
	builder.mem.Set(engine_util.CfDefault, []byte{255}, []byte{42, 5})

	var req kvrpcpb.RawScanRequest
	req.StartKey = []byte{101}
	req.Limit = 3
	req.Cf = engine_util.CfDefault
	get := NewRawScan(&req)

	resp := builder.runOneCmd(&get).(*kvrpcpb.RawScanResponse)
	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{101}, {102}, {105}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, []byte{42, byte(i + 2)}, kv.Value)
	}
}
