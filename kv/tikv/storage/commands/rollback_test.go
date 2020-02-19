package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyRollback tests a rollback with no keys.
func TestEmptyRollback(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest())
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestRollback tests a successful rollback.
func TestRollback(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}))

	builder.init([]kv{
		// See TestSinglePrewrite.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackDuplicateKeys tests a rollback which rolls back multiple keys, including one duplicated key.
func TestRollbackDuplicateKeys(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}, []byte{15}, []byte{3}))

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{15}, value: []byte{0}},
		{cf: engine_util.CfLock, key: []byte{15}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{15}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackMissingPrewrite tests trying to roll back a missing prewrite.
func TestRollbackMissingPrewrite(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackCommitted tests trying to roll back a transaction which is already committed.
func TestRollbackCommitted(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}))

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.NotNil(t, resp.Error.Abort)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
	})
}

// TestRollbackDuplicate tests trying to roll back a transaction which has already been rolled back.
func TestRollbackDuplicate(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}))

	builder.init([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100},
	})
}

// TestRollbackOtherTxn tests trying to roll back the wrong transaction.
func TestRollbackOtherTxn(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewRollback(builder.rollbackRequest([]byte{3}))

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 80}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80},
		{cf: engine_util.CfLock, key: []byte{3}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

func (builder *testBuilder) rollbackRequest(keys ...[]byte) *kvrpcpb.BatchRollbackRequest {
	var req kvrpcpb.BatchRollbackRequest
	req.StartVersion = builder.nextTs()
	req.Keys = keys
	return &req
}
