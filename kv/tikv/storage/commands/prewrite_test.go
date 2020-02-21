package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyPrewrite tests that a Prewrite with no mutations succeeds and changes nothing.
func TestEmptyPrewrite(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest())
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLen(engine_util.CfDefault, 0)
}

// TestSinglePrewrite tests a prewrite with one write, it should succeed, we test all the expected values.
func TestSinglePrewrite(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	cmd.request.LockTtl = 1000
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 3, 232}},
	})
}

// TestPrewriteLocked tests that two prewrites to the same key causes a lock error.
func TestPrewriteLocked(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	cmd2 := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{53}, kvrpcpb.Op_Put)))
	resps := builder.runCommands(&cmd, &cmd2)

	assert.Empty(t, resps[0].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[0].(*kvrpcpb.PrewriteResponse).RegionError)
	assert.Equal(t, len(resps[1].(*kvrpcpb.PrewriteResponse).Errors), 1)
	assert.Nil(t, resps[1].(*kvrpcpb.PrewriteResponse).RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestPrewriteWritten tests an attempted prewrite with a write conflict.
func TestPrewriteWritten(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 101, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Equal(t, 1, len(resp.Errors))
	assert.NotNil(t, resp.Errors[0].Conflict)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
	})
}

// TestPrewriteWrittenNoConflict tests an attempted prewrite with a write already present, but no conflict.
func TestPrewriteWrittenNoConflict(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 90, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
	})
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 1, 1)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{5}, ts: 80},
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestMultiplePrewrites tests that multiple prewrites to different keys succeeds.
func TestMultiplePrewrites(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	cmd2 := NewPrewrite(builder.prewriteRequest(mutation(4, []byte{53}, kvrpcpb.Op_Put)))
	resps := builder.runCommands(&cmd, &cmd2)

	assert.Empty(t, resps[0].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[0].(*kvrpcpb.PrewriteResponse).RegionError)
	assert.Empty(t, resps[1].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[1].(*kvrpcpb.PrewriteResponse).RegionError)
	builder.assertLens(2, 2, 0)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 101, value: []byte{53}},
		{cf: engine_util.CfLock, key: []byte{4}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestPrewriteOverwrite tests that two writes in the same prewrite succeed and we see the second write.
func TestPrewriteOverwrite(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put), mutation(3, []byte{45}, kvrpcpb.Op_Put)))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{45}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestPrewriteMultiple tests that a prewrite with multiple mutations succeeds.
func TestPrewriteMultiple(t *testing.T) {
	builder := newBuilder(t)
	cmd := NewPrewrite(builder.prewriteRequest(
		mutation(3, []byte{42}, kvrpcpb.Op_Put),
		mutation(4, []byte{43}, kvrpcpb.Op_Put),
		mutation(5, []byte{44}, kvrpcpb.Op_Put),
		mutation(4, nil, kvrpcpb.Op_Del),
		mutation(4, []byte{1, 3, 5}, kvrpcpb.Op_Put),
		mutation(255, []byte{45}, kvrpcpb.Op_Put),
	))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(4, 4, 0)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{4}, value: []byte{1, 3, 5}},
	})
}

func (builder *testBuilder) prewriteRequest(muts ...*kvrpcpb.Mutation) *kvrpcpb.PrewriteRequest {
	var req kvrpcpb.PrewriteRequest
	req.PrimaryLock = []byte{1}
	req.StartVersion = builder.nextTs()
	req.Mutations = muts
	return &req
}

func mutation(key byte, value []byte, op kvrpcpb.Op) *kvrpcpb.Mutation {
	var mut kvrpcpb.Mutation
	mut.Key = []byte{key}
	mut.Value = value
	mut.Op = op
	return &mut
}
