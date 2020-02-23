package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"sync"

	"testing"
)

// TestSeqScheduled tests that the sequential scheduler schedules multiple commands sent to it and returns the results
// in order.
func TestSeqScheduled(t *testing.T) {
	seq := NewSeqScheduler(inner_server.NewMemInnerServer())
	var chs []<-chan interfaces.SchedResult
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

func (dc *dummyCmd) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	return dc.id, nil
}

func (dc *dummyCmd) Context() *kvrpcpb.Context {
	return nil
}

func (dc *dummyCmd) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{}, nil
}

func TestAcquireLatches(t *testing.T) {
	seq := Sequential{
		latches: make(map[string]*sync.WaitGroup),
	}

	// Acquiring a new latch is ok.
	wg := seq.acquireLatches([][]byte{{}, {3}, {3, 0, 42}})
	assert.Nil(t, wg)

	// Can only acquire once.
	wg = seq.acquireLatches([][]byte{{}})
	assert.NotNil(t, wg)
	wg = seq.acquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)

	// Release then acquire is ok.
	seq.releaseLatches([][]byte{{3}, {3, 0, 43}})
	wg = seq.acquireLatches([][]byte{{3}})
	assert.Nil(t, wg)
	wg = seq.acquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)
}
