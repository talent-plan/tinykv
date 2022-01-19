package transaction

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestGetValue4B getting a value works in the simple case.
func TestGetValue4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
	})

	var req kvrpcpb.GetRequest
	req.Key = []byte{99}
	req.Version = mvcc.TsMax
	resp := builder.runOneRequest(&req).(*kvrpcpb.GetResponse)

	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte{42}, resp.Value)
}

// TestGetValueTs4B getting a value works with different timestamps.
func TestGetValueTs4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
	})

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 100
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 100
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 100

	resps := builder.runRequests(&req0, &req1, &req2)
	resp0 := resps[0].(*kvrpcpb.GetResponse)
	resp1 := resps[1].(*kvrpcpb.GetResponse)
	resp2 := resps[2].(*kvrpcpb.GetResponse)
	assert.Nil(t, resp0.RegionError)
	assert.Nil(t, resp0.Error)
	assert.Equal(t, []byte{42}, resp0.Value)
	assert.Nil(t, resp1.RegionError)
	assert.Nil(t, resp1.Error)
	assert.Equal(t, []byte{42}, resp1.Value)
	assert.Nil(t, resp2.RegionError)
	assert.Nil(t, resp2.Error)
	assert.Equal(t, []byte{42}, resp2.Value)
}

// TestGetEmpty4B tests that get on an empty DB.
func TestGetEmpty4B(t *testing.T) {
	builder := newBuilder(t)

	var req kvrpcpb.GetRequest
	req.Key = []byte{100}
	req.Version = mvcc.TsMax
	resp := builder.runOneRequest(&req).(*kvrpcpb.GetResponse)

	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.True(t, resp.NotFound)
}

// TestGetNone4B tests that getting a missing key works.
func TestGetNone4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfDefault, key: []byte{101}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{101}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
	})

	var req kvrpcpb.GetRequest
	req.Key = []byte{100}
	req.Version = mvcc.TsMax

	resp := builder.runOneRequest(&req).(*kvrpcpb.GetResponse)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.True(t, resp.NotFound)
}

// TestGetVersions4B tests we get the correct value when there are multiple versions.
func TestGetVersions4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 60, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 66, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 60}},
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 120, value: []byte{44}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 122, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 120}},
	})

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 40
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 56
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 60
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 65
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 66
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 100

	resps := builder.runRequests(&req0, &req1, &req2, &req3, &req4, &req5)
	resp0 := resps[0].(*kvrpcpb.GetResponse)
	resp1 := resps[1].(*kvrpcpb.GetResponse)
	resp2 := resps[2].(*kvrpcpb.GetResponse)
	resp3 := resps[3].(*kvrpcpb.GetResponse)
	resp4 := resps[4].(*kvrpcpb.GetResponse)
	resp5 := resps[5].(*kvrpcpb.GetResponse)

	assert.Nil(t, resp0.RegionError)
	assert.Nil(t, resp0.Error)
	assert.True(t, resp0.NotFound)
	assert.Nil(t, resp1.RegionError)
	assert.Nil(t, resp1.Error)
	assert.Equal(t, []byte{42}, resp1.Value)
	assert.Nil(t, resp2.RegionError)
	assert.Nil(t, resp2.Error)
	assert.Equal(t, []byte{42}, resp2.Value)
	assert.Nil(t, resp3.RegionError)
	assert.Nil(t, resp3.Error)
	assert.Equal(t, []byte{42}, resp3.Value)
	assert.Nil(t, resp4.RegionError)
	assert.Nil(t, resp4.Error)
	assert.Equal(t, []byte{43}, resp4.Value)
	assert.Nil(t, resp5.RegionError)
	assert.Nil(t, resp5.Error)
	assert.Equal(t, []byte{43}, resp5.Value)
}

// TestGetDeleted4B tests we get the correct value when there are multiple versions, including a deletion.
func TestGetDeleted4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 60, value: nil},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 66, value: []byte{2, 0, 0, 0, 0, 0, 0, 0, 60}},
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 120, value: []byte{44}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 122, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 120}},
	})

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 54
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 60
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 65
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 66
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 67
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 122

	resps := builder.runRequests(&req0, &req1, &req2, &req3, &req4, &req5)
	resp0 := resps[0].(*kvrpcpb.GetResponse)
	resp1 := resps[1].(*kvrpcpb.GetResponse)
	resp2 := resps[2].(*kvrpcpb.GetResponse)
	resp3 := resps[3].(*kvrpcpb.GetResponse)
	resp4 := resps[4].(*kvrpcpb.GetResponse)
	resp5 := resps[5].(*kvrpcpb.GetResponse)

	assert.Nil(t, resp0.RegionError)
	assert.Nil(t, resp0.Error)
	assert.Equal(t, []byte{42}, resp0.Value)
	assert.Nil(t, resp1.RegionError)
	assert.Nil(t, resp1.Error)
	assert.Equal(t, []byte{42}, resp1.Value)
	assert.Nil(t, resp2.RegionError)
	assert.Nil(t, resp2.Error)
	assert.Equal(t, []byte{42}, resp2.Value)
	assert.Nil(t, resp3.RegionError)
	assert.Nil(t, resp3.Error)
	assert.True(t, resp3.NotFound)
	assert.Nil(t, resp4.RegionError)
	assert.Nil(t, resp4.Error)
	assert.True(t, resp4.NotFound)
	assert.Nil(t, resp5.RegionError)
	assert.Nil(t, resp5.Error)
	assert.Equal(t, []byte{44}, resp5.Value)
}

// TestGetLocked4B tests getting a value when it is locked by another transaction.
func TestGetLocked4B(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfLock, key: []byte{99}, value: []byte{99, 1, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0}},
	})

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 55
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 300

	resps := builder.runRequests(&req0, &req1)
	resp0 := resps[0].(*kvrpcpb.GetResponse)
	resp1 := resps[1].(*kvrpcpb.GetResponse)

	assert.Nil(t, resp0.RegionError)
	assert.Nil(t, resp0.Error)
	assert.Equal(t, []byte{42}, resp0.Value)

	assert.Nil(t, resp1.RegionError)
	lockInfo := resp1.Error.Locked
	assert.Equal(t, []byte{99}, lockInfo.Key)
	assert.Equal(t, []byte{99}, lockInfo.PrimaryLock)
	assert.Equal(t, uint64(200), lockInfo.LockVersion)
}

// TestEmptyPrewrite4B tests that a Prewrite with no mutations succeeds and changes nothing.
func TestEmptyPrewrite4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest()
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLen(engine_util.CfDefault, 0)
}

// TestSinglePrewrite4B tests a prewrite with one write, it should succeed, we test all the expected values.
func TestSinglePrewrite4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put))
	cmd.LockTtl = 1000
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 3, 232}},
	})
}

// TestPrewriteLocked4B tests that two prewrites to the same key causes a lock error.
func TestPrewriteLocked4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put))
	cmd2 := builder.prewriteRequest(mutation(3, []byte{53}, kvrpcpb.Op_Put))
	resps := builder.runRequests(cmd, cmd2)

	assert.Empty(t, resps[0].(*kvrpcpb.PrewriteResponse).Errors)
	assert.Nil(t, resps[0].(*kvrpcpb.PrewriteResponse).RegionError)
	assert.Equal(t, 1, len(resps[1].(*kvrpcpb.PrewriteResponse).Errors))
	assert.Nil(t, resps[1].(*kvrpcpb.PrewriteResponse).RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestPrewriteWritten4B tests an attempted prewrite with a write conflict.
func TestPrewriteWritten4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 101, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

	assert.Equal(t, 1, len(resp.Errors))
	assert.NotNil(t, resp.Errors[0].Conflict)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
	})
}

// TestPrewriteWrittenNoConflict4B tests an attempted prewrite with a write already present, but no conflict.
func TestPrewriteWrittenNoConflict4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put))
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{5}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 90, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

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

// TestMultiplePrewrites4B tests that multiple prewrites to different keys succeeds.
func TestMultiplePrewrites4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put))
	cmd2 := builder.prewriteRequest(mutation(4, []byte{53}, kvrpcpb.Op_Put))
	resps := builder.runRequests(cmd, cmd2)

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

// TestPrewriteOverwrite4B tests that two writes in the same prewrite succeed and we see the second write.
func TestPrewriteOverwrite4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(mutation(3, []byte{42}, kvrpcpb.Op_Put), mutation(3, []byte{45}, kvrpcpb.Op_Put))
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{45}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
}

// TestPrewriteMultiple4B tests that a prewrite with multiple mutations succeeds.
func TestPrewriteMultiple4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.prewriteRequest(
		mutation(3, []byte{42}, kvrpcpb.Op_Put),
		mutation(4, []byte{43}, kvrpcpb.Op_Put),
		mutation(5, []byte{44}, kvrpcpb.Op_Put),
		mutation(4, nil, kvrpcpb.Op_Del),
		mutation(4, []byte{1, 3, 5}, kvrpcpb.Op_Put),
		mutation(255, []byte{45}, kvrpcpb.Op_Put),
	)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.PrewriteResponse)

	assert.Empty(t, resp.Errors)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(4, 4, 0)

	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{4}, value: []byte{1, 3, 5}},
	})
}

// TestEmptyCommit4B tests a commit request with no keys to commit.
func TestEmptyCommit4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([][]byte{}...)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestSimpleCommit4B tests committing a single key.
func TestSingleCommit4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitOverwrite4B tests committing where there is already a write.
func TestCommitOverwrite4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		// A previous, committed write.
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// The current, pre-written write.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitMultipleKeys4B tests committing multiple keys in the same commit. Also puts some other data in the DB and test
// that it is unchanged.
func TestCommitMultipleKeys4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3}, []byte{12, 4, 0}, []byte{15})
	builder.init([]kv{
		// Current, pre-written.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{12, 4, 0}, value: []byte{1, 1, 0, 0, 1, 5}},
		{cf: engine_util.CfLock, key: []byte{12, 4, 0}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{15}, value: []byte{0}},
		{cf: engine_util.CfLock, key: []byte{15}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},

		// Some committed data.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80, value: []byte{150}},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// Another pre-written transaction.
		{cf: engine_util.CfDefault, key: []byte{2}, ts: 99, value: []byte{0, 0, 0, 8}},
		{cf: engine_util.CfLock, key: []byte{2}, value: []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{43, 6}, ts: 99, value: []byte{1, 1, 0, 0, 1, 5}},
		{cf: engine_util.CfLock, key: []byte{43, 6}, value: []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(7, 2, 5)
	builder.assert([]kv{
		// The newly committed data.
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{12, 4, 0}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{15}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},

		// Committed data is untouched.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84},

		// Pre-written data is untouched.
		{cf: engine_util.CfDefault, key: []byte{2}, ts: 99},
		{cf: engine_util.CfLock, key: []byte{2}},
		{cf: engine_util.CfDefault, key: []byte{43, 6}, ts: 99},
		{cf: engine_util.CfLock, key: []byte{43, 6}},
	})
}

// TestRecommitKey4B tests committing the same key multiple times in one commit.
func TestRecommitKey4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3}, []byte{3})
	builder.init([]kv{
		// The current, pre-written write.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitConflictRollback4B tests committing a rolled back transaction.
func TestCommitConflictRollback4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.NotNil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
	})
}

// TestCommitConflictRace4B tests committing where a key is pre-written by a different transaction.
func TestCommitConflictRace4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 90, value: []byte{110}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 3, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.NotNil(t, resp.Error.Retryable)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfLock, key: []byte{3}},
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 90},
	})
}

// TestCommitConflictRepeat4B tests recommitting a transaction (i.e., the same commit request is received twice).
func TestCommitConflictRepeat4B(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitMissingPrewrite4a tests committing a transaction which was not prewritten (i.e., a request was lost, but
// the commit request was not).
func TestCommitMissingPrewrite4a(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		// Some committed data.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80, value: []byte{150}},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		// Note no prewrite.
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84},
	})
}
