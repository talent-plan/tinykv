package storage

import (
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

	ch := sched.Run(&get)
	result := <-ch
	sched.Stop()

	assert.Nil(t, result.Err)
	resp := result.Response.(*kvrpcpb.RawGetResponse)
	assert.Equal(t, resp.Value, []byte{42})
}
