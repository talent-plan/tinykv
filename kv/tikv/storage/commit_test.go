package storage

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestEmptyCommit(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest())
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfWrite))
}

// TestSimpleCommit tests committing a single key.
func TestSingleCommit(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	// See TestSinglePrewrite.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))

	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
}

// TestCommitOverwrite tests committing where there is already a write.
func TestCommitOverwrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()

	// A previous, committed write.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 80), []byte{15})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 84), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})

	// The current, pre-written write.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})

	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 2, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 2, mem.Len(engine_util.CfWrite))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
}

// TestMultipleKeys tests committing multiple keys in the same commit. Also puts some other data in the DB and test
// that it is unchanged.
func TestMultipleKeys(t *testing.T) {
	mem := inner_server.NewMemInnerServer()

	// Current, pre-written.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{12, 4, 0}, 100), []byte{1, 1, 0, 0, 1, 5})
	mem.Set(engine_util.CfLock, []byte{12, 4, 0}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{15}, 100), []byte{0})
	mem.Set(engine_util.CfLock, []byte{15}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})

	// Some committed data.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 80), []byte{15})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{4}, 84), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3, 0}, 80), []byte{150})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3, 0}, 84), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})

	// Another pre-written transaction.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{2}, 99), []byte{0, 0, 0, 8})
	mem.Set(engine_util.CfLock, []byte{2}, []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{43, 6}, 99), []byte{1, 1, 0, 0, 1, 5})
	mem.Set(engine_util.CfLock, []byte{43, 6}, []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99})

	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}, []byte{12, 4, 0}, []byte{15}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)

	assert.Equal(t, 7, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 2, mem.Len(engine_util.CfLock))
	assert.Equal(t, 5, mem.Len(engine_util.CfWrite))

	// The newly committed data.
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{12, 4, 0}, 110)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{15}, 110)))

	// Committed data is untouched.
	assert.Equal(t, []byte{15}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 80)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{4}, 84)))
	assert.Equal(t, []byte{150}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3, 0}, 80)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3, 0}, 84)))

	// Pre-written data is untouched.
	assert.Equal(t, []byte{0, 0, 0, 8}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{2}, 99)))
	assert.Equal(t, []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99}, mem.Get(engine_util.CfLock, []byte{2}))
	assert.Equal(t, []byte{1, 1, 0, 0, 1, 5}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{43, 6}, 99)))
	assert.Equal(t, []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99}, mem.Get(engine_util.CfLock, []byte{43, 6}))
}

// TestRecommitKey tests committing the same key multiple times in one commit.
func TestRecommitKey(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}, []byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))

	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
}

// TestConflictRollback tests committing a rolled back transaction.
func TestConflictRollback(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110), []byte{3, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	// Should be no change.
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))
	assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
}

// TestConflictRace tests committing where a key is pre-written by a different transaction.
func TestConflictRace(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 90), []byte{110})
	mem.Set(engine_util.CfLock, []byte{3}, []byte{1, 3, 0, 0, 0, 0, 0, 0, 0, 90})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.NotNil(t, resp.Error.Retryable)
	assert.Nil(t, resp.RegionError)
	// Should be no change.
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 1, mem.Len(engine_util.CfLock))
	assert.Equal(t, 0, mem.Len(engine_util.CfWrite))

	assert.Equal(t, []byte{1, 3, 0, 0, 0, 0, 0, 0, 0, 90}, mem.Get(engine_util.CfLock, []byte{3}))
	assert.Equal(t, []byte{110}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 90)))
}

// TestConflictRepeat tests recommitting a transaction (i.e., the same commit request is received twice).
func TestConflictRepeat(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100), []byte{42})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110), []byte{1, 0, 0, 0, 0, 0, 0, 0, 100})
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	// Should be no change.
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 1, mem.Len(engine_util.CfWrite))

	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3}, 110)))
	assert.Equal(t, []byte{42}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3}, 100)))
}

// TestMissingPrewrite tests committing a transaction which was not prewritten (i.e., a request was lost, but
// the commit request was not).
func TestMissingPrewrite(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	// Some committed data.
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 80), []byte{15})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{4}, 84), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})
	mem.Set(engine_util.CfDefault, kvstore.EncodeKey([]byte{3, 0}, 80), []byte{150})
	mem.Set(engine_util.CfWrite, kvstore.EncodeKey([]byte{3, 0}, 84), []byte{1, 0, 0, 0, 0, 0, 0, 0, 80})
	// Note no prewrite.
	sched := exec.NewSeqScheduler(mem)

	builder := newReqBuilder()
	cmd := commands.NewCommit(builder.commitRequest([]byte{3}))
	resp := run(t, sched, &cmd)[0].(*kvrpcpb.CommitResponse)
	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	// Should be no change to DB.
	assert.Equal(t, 2, mem.Len(engine_util.CfDefault))
	assert.Equal(t, 0, mem.Len(engine_util.CfLock))
	assert.Equal(t, 2, mem.Len(engine_util.CfWrite))

	assert.Equal(t, []byte{15}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{4}, 80)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{4}, 84)))
	assert.Equal(t, []byte{150}, mem.Get(engine_util.CfDefault, kvstore.EncodeKey([]byte{3, 0}, 80)))
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}, mem.Get(engine_util.CfWrite, kvstore.EncodeKey([]byte{3, 0}, 84)))
}

func (builder *requestBuilder) commitRequest(keys ...[]byte) *kvrpcpb.CommitRequest {
	var req kvrpcpb.CommitRequest
	req.StartVersion = builder.nextTS
	req.CommitVersion = builder.nextTS + 10
	req.Keys = keys
	builder.nextTS++
	return &req
}
