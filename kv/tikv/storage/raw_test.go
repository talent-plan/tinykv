package storage

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(nil, engine_util.CfDefault, []byte{99}, []byte{42})
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawGetRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault
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
	req.Cf = engine_util.CfDefault
	put := commands.NewRawPut(&req)

	run(t, sched, &put)
	assert.Equal(t, 1, mem.Len(engine_util.CfDefault))

	got, err := mem.Get(nil, engine_util.CfDefault, []byte{99})
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, got)
}

func TestDelete(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(nil, engine_util.CfDefault, []byte{99}, []byte{42})
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawDeleteRequest
	req.Key = []byte{99}
	req.Cf = engine_util.CfDefault
	del := commands.NewRawDelete(&req)

	run(t, sched, &del)
	assert.Equal(t, 0, mem.Len(engine_util.CfDefault))
}

func TestScan(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Set(nil, engine_util.CfDefault, []byte{99}, []byte{42})
	mem.Set(nil, engine_util.CfDefault, []byte{101}, []byte{42, 2})
	mem.Set(nil, engine_util.CfDefault, []byte{102}, []byte{42, 3})
	mem.Set(nil, engine_util.CfDefault, []byte{105}, []byte{42, 4})
	mem.Set(nil, engine_util.CfDefault, []byte{255}, []byte{42, 5})
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawScanRequest
	req.StartKey = []byte{101}
	req.Limit = 3
	req.Cf = engine_util.CfDefault
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
