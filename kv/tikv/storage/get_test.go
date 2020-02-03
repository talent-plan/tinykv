package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestGetValue getting a value works in the simple case.
func TestGetValue(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.GetRequest
	req.Key = []byte{99}
	req.Version = kvstore.TsMax
	get := commands.NewGet(&req)

	resp := run(t, sched, &get)[0].(*kvrpcpb.GetResponse)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte{42}, resp.Value)
}

// TestGetValueTs getting a value works with different timestamps.
func TestGetValueTs(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	sched := exec.NewSeqScheduler(mem)

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 100
	get0 := commands.NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 100
	get1 := commands.NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 100
	get2 := commands.NewGet(&req2)

	resps := run(t, sched, &get0, &get1, &get2)
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

// TestGetEmpty tests that get on an empty DB.
func TestGetEmpty(t *testing.T) {
	sched := exec.NewSeqScheduler(inner_server.NewMemInnerServer())

	var req kvrpcpb.GetRequest
	req.Key = []byte{100}
	req.Version = kvstore.TsMax
	get := commands.NewGet(&req)

	resp := run(t, sched, &get)[0].(*kvrpcpb.GetResponse)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte(nil), resp.Value)
}

// TestGetNone tests that getting a missing key works.
func TestGetNone(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	mem.Set(inner_server.CfDefault, []byte{42}, 101, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 101, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.GetRequest
	req.Key = []byte{100}
	req.Version = kvstore.TsMax
	get := commands.NewGet(&req)

	resp := run(t, sched, &get)[0].(*kvrpcpb.GetResponse)
	assert.Nil(t, resp.RegionError)
	assert.Nil(t, resp.Error)
	assert.Equal(t, []byte(nil), resp.Value)
}

// TestGetVersions tests we get the correct value when there are multiple versions.
func TestGetVersions(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfDefault, []byte{43}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(60))
	mem.Set(inner_server.CfDefault, []byte{44}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(120))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 60}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(66))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 120}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(122))
	sched := exec.NewSeqScheduler(mem)

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 40
	get0 := commands.NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 56
	get1 := commands.NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 60
	get2 := commands.NewGet(&req2)
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 65
	get3 := commands.NewGet(&req3)
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 66
	get4 := commands.NewGet(&req4)
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 100
	get5 := commands.NewGet(&req5)

	resps := run(t, sched, &get0, &get1, &get2, &get3, &get4, &get5)
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

// TestGetDeleted tests we get the correct value when there are multiple versions, including a deletion.
func TestGetDeleted(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfDefault, nil, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(60))
	mem.Set(inner_server.CfDefault, []byte{44}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(120))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	mem.Set(inner_server.CfWrite, []byte{2, 0, 0, 0, 0, 0, 0, 0, 60}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(66))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 120}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(122))
	sched := exec.NewSeqScheduler(mem)

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 54
	get0 := commands.NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 60
	get1 := commands.NewGet(&req1)
	var req2 kvrpcpb.GetRequest
	req2.Key = []byte{99}
	req2.Version = 65
	get2 := commands.NewGet(&req2)
	var req3 kvrpcpb.GetRequest
	req3.Key = []byte{99}
	req3.Version = 66
	get3 := commands.NewGet(&req3)
	var req4 kvrpcpb.GetRequest
	req4.Key = []byte{99}
	req4.Version = 67
	get4 := commands.NewGet(&req4)
	var req5 kvrpcpb.GetRequest
	req5.Key = []byte{99}
	req5.Version = 122
	get5 := commands.NewGet(&req5)

	resps := run(t, sched, &get0, &get1, &get2, &get3, &get4, &get5)
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

// TestGetLocked tests getting a value when it is locked by another transaction.
func TestGetLocked(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(50))
	mem.Set(inner_server.CfWrite, []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}, 99, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, ^byte(54))
	mem.Set(inner_server.CfLock, []byte{99, 0, 0, 0, 0, 0, 0, 0, 200}, 99)
	sched := exec.NewSeqScheduler(mem)

	var req0 kvrpcpb.GetRequest
	req0.Key = []byte{99}
	req0.Version = 55
	get0 := commands.NewGet(&req0)
	var req1 kvrpcpb.GetRequest
	req1.Key = []byte{99}
	req1.Version = 300
	get1 := commands.NewGet(&req1)

	resps := run(t, sched, &get0, &get1)
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
