package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyRollback tests a rollback with no keys.
func TestEmptyRollback(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest())
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfWrite))
}

// TestRollback tests a successful rollback.
func TestRollback(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	// See TestSinglePrewrite.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 100)))
}

// TestRollbackDuplicateKeys tests a rollback which rolls back multiple keys, including one duplicated key.
func TestRollbackDuplicateKeys(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{15}, 100), []byte{0})
	mem.Set(engine_util.CfLock, []byte{15}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}, []byte{15}, []byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 2, mem.Len(engine_util.CfWrite))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{15}, 100)))
}

// TestRollbackMissingPrewrite tests trying to roll back a missing prewrite.
func TestRollbackMissingPrewrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 100)))
}

// TestRollbackCommitted tests trying to roll back a transaction which is already committed (should be an error).
func TestRollbackCommitted(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110), []byte{1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.NotNil(t, resp.Error.Abort)
	assert.Nil(t, resp.RegionError)
}

// TestRollbackDuplicate tests trying to roll back a transaction which has already been rolled back.
func TestRollbackDuplicate(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110), []byte{3, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	// Should be no change.
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
}

// TestRollbackOtherTxn tests trying to roll back the wrong transaction.
func TestRollbackOtherTxn(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 80})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewRollback(builder.rollbackRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.BatchRollbackResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))
	assert.Equal(t, 1, mem.Len(engine_util.CfLock))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 80}, mem.Get(engine_util.CfLock, []byte{3}))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 100)))
}

func (builder *requestBuilder) rollbackRequest(keys ...[]byte) *kvrpcpb.BatchRollbackRequest {
	var req kvrpcpb.BatchRollbackRequest
	req.StartVersion = builder.nextTS
	req.Keys = keys
	builder.nextTS++
	return &req
}
