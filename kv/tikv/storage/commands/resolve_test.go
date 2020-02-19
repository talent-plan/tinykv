package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyResolve tests a completely empty resolve request.
func TestEmptyResolve(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().request)
	cmd.request.StartVersion = builder.nextTs()
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestResolveCommit should commit all keys in the specified transaction.
func TestResolveCommit(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 120).request)
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

// TestResolveRollback should rollback all keys in the specified transaction.
func TestResolveRollback(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 0).request)
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

// TestResolveCommit should commit the specified keys in the specified transaction.
func TestResolveCommitWithKeys(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 120).keys([]byte{3}, []byte{200}).request)
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
	builder.assertLens(3, 2, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfLock, key: []byte{7}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
	})
}

// TestResolveRollback should rollback the specified keys in the specified transaction.
func TestResolveRollbackWithKeys(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 0).keys([]byte{3}, []byte{200}).request)
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
	builder.assertLens(2, 2, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfLock, key: []byte{7}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfLock, key: []byte{200}},
		{cf: engine_util.CfWrite, key: []byte{200}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
	})
}

// TestResolveMultipleTxns should commit or rollback the specified keys in the specified transactions.
func TestResolveMultipleTxns(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 120).txn(150, 0).request)
	builder.init([]kv{
		// Should be committed.
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		// Should be committed.
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfLock, key: []byte{7}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		// Should be left alone.
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfWrite, key: []byte{200}, ts: 115, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		// Should be rolled back.
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 150, value: []byte{45}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 150, 0, 0, 0, 0, 0, 0, 0, 0}},
		// Should be rolled back.
		{cf: engine_util.CfDefault, key: []byte{201}, ts: 150, value: []byte{46}},
		{cf: engine_util.CfLock, key: []byte{201}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 150, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(3, 0, 5)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{7}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110},
		{cf: engine_util.CfWrite, key: []byte{200}, ts: 115},
		{cf: engine_util.CfWrite, key: []byte{200}, ts: 150, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 150}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 150, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 150}},
	})
}

// TestResolveCommitWritten tests a resolve where the matched keys are already committed or rolled back.
func TestResolveCommitWritten(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 120).request)
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

// TestResolveRollbackWritten tests a rollback resolve where data has already been committed or rolled back.
func TestResolveRollbackWritten(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 0).request)
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

// TestResolveCommitMissing tests a commit resolve where some of the specified keys are missing.
func TestResolveCommitMissing(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 120).keys([]byte{3}, []byte{200}).request)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100},
	})
}

// TestResolveRollbackMissing tests a rollback resolve where some of the specified keys are missing.
func TestResolveRollbackMissing(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewResolveLock(reqBuilder().txn(100, 0).keys([]byte{3}, []byte{200}).request)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{7}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ResolveLockResponse)

	assert.NotNil(t, resp.Error.Retryable)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 120},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100},
		{cf: engine_util.CfWrite, key: []byte{7}, ts: 100},
	})
}

type resolveReqBuilder struct {
	request *kvrpcpb.ResolveLockRequest
}

func reqBuilder() *resolveReqBuilder {
	return &resolveReqBuilder{&kvrpcpb.ResolveLockRequest{}}
}

func (builder *resolveReqBuilder) keys(keys ...[]byte) *resolveReqBuilder {
	builder.request.Keys = keys
	return builder
}

func (builder *resolveReqBuilder) txn(startTs uint64, commitTs uint64) *resolveReqBuilder {
	if len(builder.request.TxnInfos) == 0 {
		if builder.request.StartVersion == 0 {
			builder.request.StartVersion = startTs
			builder.request.CommitVersion = commitTs
		} else {
			builder.request.TxnInfos = []*kvrpcpb.TxnInfo{
				{Txn: builder.request.StartVersion, Status: builder.request.CommitVersion},
				{Txn: startTs, Status: commitTs},
			}
			builder.request.StartVersion = 0
			builder.request.CommitVersion = 0
		}
	} else {
		builder.request.TxnInfos = append(builder.request.TxnInfos, &kvrpcpb.TxnInfo{Txn: startTs, Status: commitTs})
	}

	return builder
}
