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
	mem.Data[99] = []byte{42}
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawGetRequest
	req.Key = []byte{99}
	req.Cf = "default"
	get := commands.NewRawGet(&req)

	resp := run(t, sched, &get).(*kvrpcpb.RawGetResponse)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestPut(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawPutRequest
	req.Key = []byte{99}
	req.Value = []byte{42}
	req.Cf = "default"
	put := commands.NewRawPut(&req)

	run(t, sched, &put)
	assert.Equal(t, 1, len(mem.Data))
	assert.Equal(t, []byte{42}, mem.Data[99])
}

func TestDelete(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Data[99] = []byte{42}
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawDeleteRequest
	req.Key = []byte{99}
	req.Cf = "default"
	del := commands.NewRawDelete(&req)

	run(t, sched, &del)
	assert.Empty(t, mem.Data)
}

func TestScan(t *testing.T) {
	mem := inner_server.NewMemInnerServer()
	mem.Data[99] = []byte{42, 1}
	mem.Data[101] = []byte{42, 2}
	mem.Data[102] = []byte{42, 3}
	mem.Data[105] = []byte{42, 4}
	mem.Data[255] = []byte{42, 5}
	sched := exec.NewSeqScheduler(mem)

	var req kvrpcpb.RawScanRequest
	req.StartKey = []byte{101}
	req.Limit = 3
	req.Cf = "default"
	get := commands.NewRawScan(&req)

	resp := run(t, sched, &get).(*kvrpcpb.RawScanResponse)
	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{101}, {102}, {105}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, kv.Key, expectedKeys[i])
		assert.Equal(t, kv.Value, []byte{42, byte(i + 2)})
	}
}

func run(t *testing.T, sched tikv.Scheduler, cmd tikv.Command) interface{} {
	ch := sched.Run(cmd)
	result := <-ch
	sched.Stop()
	assert.Nil(t, result.Err)
	return result.Response
}
