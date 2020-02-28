package transaction

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestGetValue4A getting a value works in the simple case.
func TestGetValue4A(t *testing.T) {
}

// TestGetValueTs4A getting a value works with different timestamps.
func TestGetValueTs4A(t *testing.T) {
}

// TestGetEmpty4A tests that get on an empty DB.
func TestGetEmpty4A(t *testing.T) {
}

// TestGetNone4A tests that getting a missing key works.
func TestGetNone4A(t *testing.T) {
}

// TestGetVersions4A tests we get the correct value when there are multiple versions.
func TestGetVersions4A(t *testing.T) {
}

// TestGetDeleted4A tests we get the correct value when there are multiple versions, including a deletion.
func TestGetDeleted4A(t *testing.T) {
}

// TestGetLocked4A tests getting a value when it is locked by another transaction.
func TestGetLocked4A(t *testing.T) {
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

// TestEmptyPrewrite4A tests that a Prewrite with no mutations succeeds and changes nothing.
func TestEmptyPrewrite4A(t *testing.T) {
}

// TestSinglePrewrite4A tests a prewrite with one write, it should succeed, we test all the expected values.
func TestSinglePrewrite4A(t *testing.T) {
}

// TestPrewriteLocked4A tests that two prewrites to the same key causes a lock error.
func TestPrewriteLocked4A(t *testing.T) {
}

// TestPrewriteWritten4A tests an attempted prewrite with a write conflict.
func TestPrewriteWritten4A(t *testing.T) {
}

// TestPrewriteWrittenNoConflict4A tests an attempted prewrite with a write already present, but no conflict.
func TestPrewriteWrittenNoConflict4A(t *testing.T) {
}

// TestMultiplePrewrites4A tests that multiple prewrites to different keys succeeds.
func TestMultiplePrewrites4A(t *testing.T) {
}

// TestPrewriteOverwrite4A tests that two writes in the same prewrite succeed and we see the second write.
func TestPrewriteOverwrite4A(t *testing.T) {
}

// TestPrewriteMultiple4A tests that a prewrite with multiple mutations succeeds.
func TestPrewriteMultiple4A(t *testing.T) {
}

// TestEmptyCommit4A tests a commit request with no keys to commit.
func TestEmptyCommit4A(t *testing.T) {
}

// TestSimpleCommit4A tests committing a single key.
func TestSingleCommit4A(t *testing.T) {
}

// TestCommitOverwrite4A tests committing where there is already a write.
func TestCommitOverwrite4A(t *testing.T) {
}

// TestCommitMultipleKeys4A tests committing multiple keys in the same commit. Also puts some other data in the DB and test
// that it is unchanged.
func TestCommitMultipleKeys4A(t *testing.T) {
}

// TestRecommitKey4A tests committing the same key multiple times in one commit.
func TestRecommitKey4A(t *testing.T) {
}

// TestCommitConflictRollback4A tests committing a rolled back transaction.
func TestCommitConflictRollback4A(t *testing.T) {
}

// TestCommitConflictRace4A tests committing where a key is pre-written by a different transaction.
func TestCommitConflictRace4A(t *testing.T) {
}

// TestCommitConflictRepeat4A tests recommitting a transaction (i.e., the same commit request is received twice).
func TestCommitConflictRepeat4A(t *testing.T) {
}

// TestCommitMissingPrewrite4a tests committing a transaction which was not prewritten (i.e., a request was lost, but
// the commit request was not).
func TestCommitMissingPrewrite4a(t *testing.T) {
}
