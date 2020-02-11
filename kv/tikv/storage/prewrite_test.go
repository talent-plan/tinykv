package storage

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyPrewrite tests that a Prewrite with no mutations succeeds and changes nothing.
func TestEmptyPrewrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest())
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
}

// TestSinglePrewrite tests a prewrite with one write, it should succeed, we test all the expected values.
func TestSinglePrewrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, mem.Len(engine_util.CfDefault), 1)
	assert.Equal(t, mem.Len(engine_util.CfLock), 1)

	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfLock, []byte{3}))
}

// TestPrewriteLocked tests that two prewrites to the same key causes a lock error.
func TestPrewriteLocked(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	cmd2 := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{53}, kvrpcpb.Op_Put)))
	resps := run(t, sched, &cmd, &cmd2)

	assert.Empty(t, resps[0].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[0].(*kvrpcpb.PrewriteResponse).RegionError)
	assert.Equal(t, len(resps[1].(*kvrpcpb.PrewriteResponse).Errors), 1)
	assert.Nil(t, resps[1].(*kvrpcpb.PrewriteResponse).RegionError)

	assert.Equal(t, mem.Len(engine_util.CfDefault), 1)
	assert.Equal(t, mem.Len(engine_util.CfLock), 1)
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfLock, []byte{3}))
}

// TestPrewriteWritten tests an attempted prewrite with a write conflict.
func TestPrewriteWritten(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80), []byte{5})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 101), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Equal(t, 1, len(resp.Errors))
	assert.NotNil(t, resp.Errors[0].Conflict)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, mem.Len(engine_util.CfDefault), 1)
	assert.Equal(t, mem.Len(engine_util.CfLock), 0)
	assert.Equal(t, mem.Len(engine_util.CfWrite), 1)

	assert.Equal(t, []byte{5}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80)))
}

// TestPrewriteWrittenNoConflict tests an attempted prewrite with a write already present, but no conflict.
func TestPrewriteWrittenNoConflict(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80), []byte{5})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 90), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, mem.Len(engine_util.CfDefault), 2)
	assert.Equal(t, mem.Len(engine_util.CfLock), 1)
	assert.Equal(t, mem.Len(engine_util.CfWrite), 1)

	assert.Equal(t, []byte{5}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80)))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfLock, []byte{3}))
}

// TestMultiplePrewrites tests that multiple prewrites to different keys succeeds.
func TestMultiplePrewrites(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put)))
	cmd2 := commands.NewPrewrite(builder.prewriteRequest(mutation(4, []byte{53}, kvrpcpb.Op_Put)))
	resps := run(t, sched, &cmd, &cmd2)

	assert.Empty(t, resps[0].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[0].(*kvrpcpb.PrewriteResponse).RegionError)
	assert.Empty(t, resps[1].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[1].(*kvrpcpb.PrewriteResponse).RegionError)

	assert.Equal(t, mem.Len(engine_util.CfDefault), 2)
	assert.Equal(t, mem.Len(engine_util.CfLock), 2)
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfLock, []byte{3}))
	assert.Equal(t, []byte{53}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 101)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 101}, mem.Get(engine_util.CfLock, []byte{4}))
}

// TestPrewriteOverwrite tests that two writes in the same prewrite succeed and we see the second write.
func TestPrewriteOverwrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put), mutation(3, []byte{45}, kvrpcpb.Op_Put)))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, mem.Len(engine_util.CfDefault), 1)
	assert.Equal(t, mem.Len(engine_util.CfLock), 1)

	assert.Equal(t, []byte{45}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfLock, []byte{3}))
}

// TestPrewriteMultiple tests that a prewrite with multiple mutations succeeds.
func TestPrewriteMultiple(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewPrewrite(builder.prewriteRequest(
		mutation(3, []byte{42}, kvrpcpb.Op_Put),
		mutation(4, []byte{43}, kvrpcpb.Op_Put),
		mutation(5, []byte{44}, kvrpcpb.Op_Put),
		mutation(4, nil, kvrpcpb.Op_Del),
		mutation(4, []byte{1, 3, 5}, kvrpcpb.Op_Put),
		mutation(255, []byte{45}, kvrpcpb.Op_Put),
	))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.PrewriteResponse)
	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, mem.Len(engine_util.CfDefault), 4)
	assert.Equal(t, mem.Len(engine_util.CfLock), 4)

	assert.Equal(t, []byte{1, 3, 5}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 100)))
}

type requestBuilder struct {
	nextTS uint64
}

func newReqBuilder() requestBuilder {
	return requestBuilder{
		nextTS: 100,
	}
}

func (builder *requestBuilder) prewriteRequest(muts ...*kvrpcpb.Mutation) *kvrpcpb.PrewriteRequest {
	var req kvrpcpb.PrewriteRequest
	req.PrimaryLock = []byte{1}
	req.StartVersion = builder.nextTS
	builder.nextTS++
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
