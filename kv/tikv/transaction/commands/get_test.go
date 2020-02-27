package commands

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/tikv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestGetValue4A getting a value works in the simple case.
func TestGetValue4A(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
	})

	var req kvrpcpb.GetRequest
	req.Key = []byte{99}
	req.Version = mvcc.TsMax
	cmd := NewGet(&req)
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.GetResponse)

	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte{42}, resp.Value)
}

// TestGetValueTs4A getting a value works with different timestamps.
func TestGetValueTs4A(t *testing.T) {
	builder := newBuilder(t)
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
	})

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 100
	get0 := NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 100
	get1 := NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 100
	get2 := NewGet(&req2)

	resps := builder.runCommands(&get0, &get1, &get2)
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

// TestGetEmpty4A tests that get on an empty DB.
func TestGetEmpty4A(t *testing.T) {
	builder := newBuilder(t)

	var req kvrpcpb.GetRequest
	req.Key = []byte{100}
	req.Version = mvcc.TsMax
	cmd := NewGet(&req)
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.GetResponse)

	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte(nil), resp.Value)
}

// TestGetNone4A tests that getting a missing key works.
func TestGetNone4A(t *testing.T) {
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
	cmd := NewGet(&req)

	resp := builder.runOneCmd(&cmd).(*kvrpcpb.GetResponse)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte(nil), resp.Value)
}

// TestGetVersions4A tests we get the correct value when there are multiple versions.
func TestGetVersions4A(t *testing.T) {
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
	get0 := NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 56
	get1 := NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 60
	get2 := NewGet(&req2)
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 65
	get3 := NewGet(&req3)
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 66
	get4 := NewGet(&req4)
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 100
	get5 := NewGet(&req5)

	resps := builder.runCommands(&get0, &get1, &get2, &get3, &get4, &get5)
	resp0 := resps[0].(*kvrpcpb.GetResponse)
	resp1 := resps[1].(*kvrpcpb.GetResponse)
	resp2 := resps[2].(*kvrpcpb.GetResponse)
	resp3 := resps[3].(*kvrpcpb.GetResponse)
	resp4 := resps[4].(*kvrpcpb.GetResponse)
	resp5 := resps[5].(*kvrpcpb.GetResponse)

	assert.Nil(t, resp0.RegionError)
	assert.Nil(t, resp0.Error)
	assert.Equal(t, []byte(nil), resp0.Value)
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

// TestGetDeleted4A tests we get the correct value when there are multiple versions, including a deletion.
func TestGetDeleted4A(t *testing.T) {
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
	get0 := NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 60
	get1 := NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 65
	get2 := NewGet(&req2)
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 66
	get3 := NewGet(&req3)
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 67
	get4 := NewGet(&req4)
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 122
	get5 := NewGet(&req5)

	resps := builder.runCommands(&get0, &get1, &get2, &get3, &get4, &get5)
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
	assert.Equal(t, []byte(nil), resp3.Value)
	assert.Nil(t, resp4.RegionError)
	assert.Nil(t, resp4.Error)
	assert.Equal(t, []byte(nil), resp4.Value)
	assert.Nil(t, resp5.RegionError)
	assert.Nil(t, resp5.Error)
	assert.Equal(t, []byte{44}, resp5.Value)
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
	get0 := NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 300
	get1 := NewGet(&req1)

	resps := builder.runCommands(&get0, &get1)
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
