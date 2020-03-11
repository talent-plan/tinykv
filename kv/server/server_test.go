package server

import (
	"context"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/storage"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	mem := storage.NewMemStorage()
	server := NewServer(mem)
	mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})

	var req kvrpcpb.RawGetRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault

	resp, err := server.RawGet(context.Background(), &req)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestPut(t *testing.T) {
	mem := storage.NewMemStorage()
	server := NewServer(mem)

	var req kvrpcpb.RawPutRequest
	req.Key = []byte{99}
	req.Value = []byte{42}
	req.Cf = engine_util.CfDefault

	_, err := server.RawPut(context.Background(), &req)
	assert.Nil(t, err)
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, []byte{99}))
}

func TestDelete(t *testing.T) {
	mem := storage.NewMemStorage()
	server := NewServer(mem)
	mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})

	var req kvrpcpb.RawDeleteRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault

	_, err := server.RawDelete(context.Background(), &req)
	assert.Nil(t, err)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
}

func TestScan(t *testing.T) {
	mem := storage.NewMemStorage()
	server := NewServer(mem)
	mem.Set(engine_util.CfDefault, []byte{99}, []byte{42})
	mem.Set(engine_util.CfDefault, []byte{101}, []byte{42, 2})
	mem.Set(engine_util.CfDefault, []byte{102}, []byte{42, 3})
	mem.Set(engine_util.CfDefault, []byte{105}, []byte{42, 4})
	mem.Set(engine_util.CfDefault, []byte{255}, []byte{42, 5})

	var req kvrpcpb.RawScanRequest
	req.StartKey = []byte{101}
	req.Limit = 3
	req.Cf = engine_util.CfDefault

	resp, err := server.RawScan(context.Background(), &req)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{101}, {102}, {105}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, []byte{42, byte(i + 2)}, kv.Value)
	}
}
