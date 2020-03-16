package transaction

import (
	"encoding/binary"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyRollback4C tests a rollback with no keys.
func TestEmptyRollback4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([][]byte{}...)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestRollback4C tests a successful rollback.
func TestRollback4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3})

	builder.init([]kv{
		// See TestSinglePrewrite.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackDuplicateKeys4C tests a rollback which rolls back multiple keys, including one duplicated key.
func TestRollbackDuplicateKeys4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3}, []byte{15}, []byte{3})

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{15}, value: []byte{0}},
		{cf: engine_util.CfLock, key: []byte{15}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{15}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackMissingPrewrite4C tests trying to roll back a missing prewrite.
func TestRollbackMissingPrewrite4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestRollbackCommitted4C tests trying to roll back a transaction which is already committed.
func TestRollbackCommitted4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3})

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.NotNil(t, resp.Error.Abort)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
	})
}

// TestRollbackDuplicate4C tests trying to roll back a transaction which has already been rolled back.
func TestRollbackDuplicate4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3})

	builder.init([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100},
	})
}

// TestRollbackOtherTxn4C tests trying to roll back the wrong transaction.
func TestRollbackOtherTxn4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.rollbackRequest([]byte{3})

	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 80, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.BatchRollbackResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80},
		{cf: engine_util.CfLock, key: []byte{3}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestCheckTxnStatusTtlExpired4C checks that if there is a lock and its ttl has expired, then it is rolled back.
func TestCheckTxnStatusTtlExpired4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.checkTxnStatusRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 5, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 8}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_TTLExpireRollback, resp.Action)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestCheckTxnStatusTtlNotExpired4C checks that if there is a lock and its ttl has not expired, then nothing changes.
func TestCheckTxnStatusTtlNotExpired4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.checkTxnStatusRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 5, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 1, 0, 0, 0, 8}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs},
		{cf: engine_util.CfLock, key: []byte{3}},
	})
}

// TestCheckTxnStatusRolledBack4C tests checking a key which has already been rolled back..
func TestCheckTxnStatusRolledBack4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.checkTxnStatusRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{3, 1, 0, 0, 8, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 8}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	assert.Equal(t, uint64(0), resp.CommitVersion)
	builder.assertLens(1, 1, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs},
		{cf: engine_util.CfLock, key: []byte{3}},
	})
}

// TestCheckTxnStatusCommitted4C tests checking a key which has already been committed.
func TestCheckTxnStatusCommitted4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.checkTxnStatusRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs, value: []byte{1, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_NoAction, resp.Action)
	assert.Equal(t, binary.BigEndian.Uint64([]byte{0, 0, 5, 0, 0, 0, 0, builder.ts()}), resp.CommitVersion)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: cmd.LockTs},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs},
	})
}

// TestCheckTxnStatusNoLockNoWrite4C checks if there is no data for the key, then we get the right response.
func TestCheckTxnStatusNoLockNoWrite4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.checkTxnStatusRequest([]byte{3})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CheckTxnStatusResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, kvrpcpb.Action_LockNotExistRollback, resp.Action)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: cmd.LockTs, value: []byte{3, 0, 0, 5, 0, 0, 0, 0, builder.ts()}},
	})
}

// TestEmptyResolve4C tests a completely empty resolve request.
func TestEmptyResolve4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := resolveRequest(0, 0)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ResolveLockResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestResolveCommit4C should commit all keys in the specified transaction.
func TestResolveCommit4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := resolveRequest(100, 120)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfLock, key: []byte{7}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ResolveLockResponse)

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

// TestResolveRollback4C should rollback all keys in the specified transaction.
func TestResolveRollback4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := resolveRequest(100, 0)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfLock, key: []byte{7}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ResolveLockResponse)

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

// TestResolveCommitWritten4C tests a resolve where the matched keys are already committed or rolled back.
func TestResolveCommitWritten4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := resolveRequest(100, 120)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ResolveLockResponse)

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

// TestResolveRollbackWritten4C tests a rollback resolve where data has already been committed or rolled back.
func TestResolveRollbackWritten4C(t *testing.T) {
	builder := newBuilder(t)
	cmd := resolveRequest(100, 0)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 100, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 120, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{7}, ts: 100, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{201}, ts: 100, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, 100}},
		{cf: engine_util.CfDefault, key: []byte{200}, ts: 110, value: []byte{44}},
		{cf: engine_util.CfLock, key: []byte{200}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ResolveLockResponse)

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

// TestScanEmpty4C tests a scan after the end of the DB.
func TestScanEmpty4C(t *testing.T) {
	builder := builderForScan(t)

	cmd := builder.scanRequest([]byte{200}, 10000)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Empty(t, resp.Pairs)
}

// TestScanLimitZero4C tests we get nothing if limit is 0.
func TestScanLimitZero4C(t *testing.T) {
	builder := builderForScan(t)

	cmd := builder.scanRequest([]byte{3}, 0)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Empty(t, resp.Pairs)
}

// TestScanAll4C start at the beginning of the DB and read all pairs, respecting the timestamp.
func TestScanAll4C(t *testing.T) {
	builder := builderForScan(t)

	cmd := builder.scanRequest([]byte{0}, 10000)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ScanResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 11, len(resp.Pairs))
	assert.Equal(t, []byte{1}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{50}, resp.Pairs[0].Value)
	assert.Equal(t, []byte{199}, resp.Pairs[10].Key)
	assert.Equal(t, []byte{54}, resp.Pairs[10].Value)
}

// TestScanLimit4C tests that scan takes the limit into account.
func TestScanLimit4C(t *testing.T) {
	builder := builderForScan(t)

	cmd := builder.scanRequest([]byte{2}, 6)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.ScanResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 6, len(resp.Pairs))
	assert.Equal(t, []byte{3}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{51}, resp.Pairs[0].Value)
	assert.Equal(t, []byte{4}, resp.Pairs[5].Key)
	assert.Equal(t, []byte{52}, resp.Pairs[5].Value)
}

// TestScanDeleted4C scan over a value which is deleted then replaced.
func TestScanDeleted4C(t *testing.T) {
	builder := builderForScan(t)

	req1 := builder.scanRequest([]byte{100}, 10000)
	req1.Version = 100
	req2 := builder.scanRequest([]byte{100}, 10000)
	req2.Version = 105
	req3 := builder.scanRequest([]byte{100}, 10000)
	req3.Version = 120

	resps := builder.runRequests(req1, req2, req3)

	resp1 := resps[0].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp1.RegionError)
	assert.Equal(t, 3, len(resp1.Pairs))
	assert.Equal(t, []byte{150}, resp1.Pairs[1].Key)
	assert.Equal(t, []byte{42}, resp1.Pairs[1].Value)

	resp2 := resps[1].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp2.RegionError)
	assert.Equal(t, 2, len(resp2.Pairs))
	assert.Equal(t, []byte{120}, resp2.Pairs[0].Key)
	assert.Equal(t, []byte{199}, resp2.Pairs[1].Key)

	resp3 := resps[2].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp3.RegionError)
	assert.Equal(t, 3, len(resp3.Pairs))
	assert.Equal(t, []byte{150}, resp3.Pairs[1].Key)
	assert.Equal(t, []byte{64}, resp3.Pairs[1].Value)
}

func builderForScan(t *testing.T) *testBuilder {
	values := []kv{
		// Committed before 100.
		{engine_util.CfDefault, []byte{1}, 80, []byte{50}},
		{engine_util.CfWrite, []byte{1}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{1, 23}, 80, []byte{55}},
		{engine_util.CfWrite, []byte{1, 23}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3}, 80, []byte{51}},
		{engine_util.CfWrite, []byte{3}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 45}, 80, []byte{56}},
		{engine_util.CfWrite, []byte{3, 45}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 46}, 80, []byte{57}},
		{engine_util.CfWrite, []byte{3, 46}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 47}, 80, []byte{58}},
		{engine_util.CfWrite, []byte{3, 47}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 48}, 80, []byte{59}},
		{engine_util.CfWrite, []byte{3, 48}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{4}, 80, []byte{52}},
		{engine_util.CfWrite, []byte{4}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{120}, 80, []byte{53}},
		{engine_util.CfWrite, []byte{120}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{199}, 80, []byte{54}},
		{engine_util.CfWrite, []byte{199}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// Committed after 100.
		{engine_util.CfDefault, []byte{4, 45}, 110, []byte{58}},
		{engine_util.CfWrite, []byte{4, 45}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 46}, 110, []byte{57}},
		{engine_util.CfWrite, []byte{4, 46}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 47}, 110, []byte{58}},
		{engine_util.CfWrite, []byte{4, 47}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 48}, 110, []byte{59}},
		{engine_util.CfWrite, []byte{4, 48}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},

		// Committed after 100, but started before.
		{engine_util.CfDefault, []byte{5, 45}, 97, []byte{60}},
		{engine_util.CfWrite, []byte{5, 45}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 46}, 97, []byte{61}},
		{engine_util.CfWrite, []byte{5, 46}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 47}, 97, []byte{62}},
		{engine_util.CfWrite, []byte{5, 47}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 48}, 97, []byte{63}},
		{engine_util.CfWrite, []byte{5, 48}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},

		// A deleted value and replaced value.
		{engine_util.CfDefault, []byte{150}, 80, []byte{42}},
		{engine_util.CfWrite, []byte{150}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfWrite, []byte{150}, 101, []byte{2, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{150}, 110, []byte{64}},
		{engine_util.CfWrite, []byte{150}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
	}
	builder := newBuilder(t)
	builder.init(values)
	return &builder
}
