package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyResolve4B tests a completely empty resolve request.
func TestEmptyResolve4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(resolveRequest(0, 0))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestResolveCommit4B should commit all keys in the specified transaction.
func TestResolveCommit4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(resolveRequest(100, 120))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfLock, key: []byte{7}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(3, 1, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{7}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
	})
}

// TestResolveRollback4B should rollback all keys in the specified transaction.
func TestResolveRollback4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(resolveRequest(100, 0))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfLock, key: []byte{7}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfWrite, key: []byte{7}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
	})
}

// TestResolveCommitWritten4B tests a resolve where the matched keys are already committed or rolled back.
func TestResolveCommitWritten4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(resolveRequest(100, 120))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(3, 1, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
	})
}

// TestResolveRollbackWritten4B tests a rollback resolve where data has already been committed or rolled back.
func TestResolveRollbackWritten4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(resolveRequest(100, 0))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(3, 1, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
	})
}

func resolveRequest(startTs uint64, commitTs uint64) *kvrpcpb.ResolveLockRequest {
	var req kvrpcpb.ResolveLockRequest
	req.StartVersion = startTs
	req.CommitVersion = commitTs
	return &req
}
