package commands

import (
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestCheckTxnStatusTtlExpired checks that if there is a lock and its ttl has expired, then it is rolled back.
func TestCheckTxnStatusTtlExpired(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewCheckTxnStatus(builder.checkTxnStatusRequest([]byte{3}))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 5, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 8}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_TTLExpireRollback, resp.Action)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestCheckTxnStatusTtlNotExpired checks that if there is a lock and its ttl has not expired, then nothing changes.
func TestCheckTxnStatusTtlNotExpired(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewCheckTxnStatus(builder.checkTxnStatusRequest([]byte{3}))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 5, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 1, 0, 0, 0, 8}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs},
		{cf: engine_util.CfLock, key: []byte{3}},
	})
}

// TestCheckTxnStatusRolledBack tests checking a key which has already been rolled back..
func TestCheckTxnStatusRolledBack(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewCheckTxnStatus(builder.checkTxnStatusRequest([]byte{3}))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 8, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 8}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	builder.assertLens(1, 1, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs},
		{cf: engine_util.CfLock, key: []byte{3}},
	})
}

// TestCheckTxnStatusCommitted tests checking a key which has already been committed.
func TestCheckTxnStatusCommitted(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewCheckTxnStatus(builder.checkTxnStatusRequest([]byte{3}))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{1, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	assert.Equal(t, binary.BigEndian.Uint64([]byte{0, 0, 5, 0, 0, 0, 0, builder.ts()}), resp.CommitVersion)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.request.LockTs},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs},
	})
}

// TestCheckTxnStatusNoLockNoWrite checks if there is no data for the key, then we get the right response.
func TestCheckTxnStatusNoLockNoWrite(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewCheckTxnStatus(builder.checkTxnStatusRequest([]byte{3}))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_LockNotExistRollback, resp.Action)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.request.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
}

func (builder *testBuilder) checkTxnStatusRequest(key []byte) *kvrpcpb.CheckTxnStatusRequest {
	var req kvrpcpb.CheckTxnStatusRequest
	builder.nextTs()
	req.LockTs = binary.BigEndian.Uint64([]byte{0, 0, 5, 0, 0, 0, 0, builder.ts()})
	req.CurrentTs = binary.BigEndian.Uint64([]byte{0, 0, 6, 0, 0, 0, 0, builder.ts()})
	req.PrimaryKey = key
	return &req
}
