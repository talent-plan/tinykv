package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"

	"testing"
)

// TestSeqScheduled tests that the sequential scheduler schedules multiple commands sent to it and returns the results
// in order.
func TestSeqScheduled(t *testing.T) {
	seq := NewSeqScheduler(inner_server.NewMemInnerServer())
	var chs []<-chan tikv.RespResult
	for i := 0; i < 6; i++ {
		chs = append(chs, seq.Run(&dummyCmd{i}))
	}

	for i, ch := range chs {
		r := <-ch
		assert.Equal(t, r.Response.(int), i)
	}
	seq.Stop()
}

type dummyCmd struct {
	id int
}

func (dc *dummyCmd) BuildTxn(txn *kvstore.MvccTxn) error {
	return nil
}

func (dc *dummyCmd) Context() *kvrpcpb.Context {
	return nil
}

func (dc *dummyCmd) Response() interface{} {
	return dc.id
}

func (dc *dummyCmd) HandleError(err error) interface{} {
	return nil
}
