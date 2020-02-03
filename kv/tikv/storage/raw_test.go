package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGet(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99)
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawGetRequest
	req.Key = []byte{99}
	req.Cf = inner_server.CfDefault
	get := commands.NewRawGet(&req)

	resp := run(t, sched, &get)[0].(*kvrpcpb.RawGetResponse)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestPut(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawPutRequest
	req.Key = []byte{99}
	req.Value = []byte{42}
	req.Cf = inner_server.CfDefault
	put := commands.NewRawPut(&req)

	run(t, sched, &put)
	assert.Equal(t, 1, mem.Len(inner_server.CfDefault))
	assert.Equal(t, []byte{42}, mem.Get(inner_server.CfDefault, 99))
}

func TestDelete(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99)
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawDeleteRequest
	req.Key = []byte{99}
	req.Cf = inner_server.CfDefault
	del := commands.NewRawDelete(&req)

	run(t, sched, &del)
	assert.Equal(t, 0, mem.Len(inner_server.CfDefault))
}

func TestScan(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(inner_server.CfDefault, []byte{42}, 99)
	mem.Set(inner_server.CfDefault, []byte{42, 2}, 101)
	mem.Set(inner_server.CfDefault, []byte{42, 3}, 102)
	mem.Set(inner_server.CfDefault, []byte{42, 4}, 105)
	mem.Set(inner_server.CfDefault, []byte{42, 5}, 255)
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawScanRequest
	req.StartKey = []byte{101}
	req.Limit = 3
	req.Cf = inner_server.CfDefault
	get := commands.NewRawScan(&req)

	resp := run(t, sched, &get)[0].(*kvrpcpb.RawScanResponse)
	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{101}, {102}, {105}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, []byte{42, byte(i + 2)}, kv.Value)
	}
}

func run(t *testing.T, sched tikv.Scheduler, cmds ...tikv.Command) []interface{} {
	var result []interface{}
	for _, c := range cmds {
		ch := sched.Run(c)
		r := <-ch
		assert.Nil(t, r.Err)
		result = append(result, r.Response)
	}
	sched.Stop()
	return result
}
