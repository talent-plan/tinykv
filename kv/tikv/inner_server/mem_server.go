package inner_server

import (
	"bytes"
	"fmt"

	"github.com/coocood/badger/y"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

// MemInnerServer is a simple inner server backed by memory for testing. Data is not written to disk, nor sent to other
// nodes. It is intended for testing only.
type MemInnerServer struct {
	CfDefault *llrb.LLRB
	CfLock    *llrb.LLRB
	CfWrite   *llrb.LLRB
}

func NewMemInnerServer() *MemInnerServer {
	return &MemInnerServer{
		CfDefault: llrb.New(),
		CfLock:    llrb.New(),
		CfWrite:   llrb.New(),
	}
}

func (is *MemInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

func (is *MemInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func (is *MemInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *MemInnerServer) Stop() error {
	return nil
}

func (is *MemInnerServer) Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error) {
	return &memReader{is}, nil
}

func (is *MemInnerServer) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := memItem{data.Key, data.Value}
			switch data.Cf {
			case engine_util.CfDefault:
				is.CfDefault.ReplaceOrInsert(item)
			case engine_util.CfLock:
				is.CfLock.ReplaceOrInsert(item)
			case engine_util.CfWrite:
				is.CfWrite.ReplaceOrInsert(item)
			}
		case Delete:
			item := memItem{data.Key, nil}
			switch data.Cf {
			case engine_util.CfDefault:
				is.CfDefault.Delete(item)
			case engine_util.CfLock:
				is.CfLock.Delete(item)
			case engine_util.CfWrite:
				is.CfWrite.Delete(item)
			}
		}
	}

	return nil
}

func (is *MemInnerServer) Get(ctx *kvrpcpb.Context, cf string, key []byte) ([]byte, error) {
	reader, err := is.Reader(nil)
	if err != nil {
		return nil, err
	}
	return reader.GetCF(cf, key)
}

func (is *MemInnerServer) Set(ctx *kvrpcpb.Context, cf string, key []byte, value []byte) error {
	return is.Write(ctx, []Modify{
		{
			Type: ModifyTypePut,
			Data: Put{
				Cf:    cf,
				Key:   key,
				Value: value,
			},
		},
	})
}

func (is *MemInnerServer) MustGet(ctx *kvrpcpb.Context, cf string, key []byte) []byte {
	if value, err := is.Get(ctx, cf, key); err != nil {
		panic(err)
	} else {
		return value
	}
}

func (is *MemInnerServer) Len(cf string) int {
	switch cf {
	case engine_util.CfDefault:
		return is.CfDefault.Len()
	case engine_util.CfLock:
		return is.CfLock.Len()
	case engine_util.CfWrite:
		return is.CfWrite.Len()
	}

	return -1
}

// memReader is a DBReader which reads from a MemInnerServer.
type memReader struct {
	inner *MemInnerServer
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	item := memItem{key, nil}
	var result llrb.Item
	switch cf {
	case engine_util.CfDefault:
		result = mr.inner.CfDefault.Get(item)
	case engine_util.CfLock:
		result = mr.inner.CfLock.Get(item)
	case engine_util.CfWrite:
		result = mr.inner.CfWrite.Get(item)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}

	if result == nil {
		return nil, nil
	}

	return result.(memItem).value, nil
}

func (mr *memReader) IterCF(cf string) engine_util.DBIterator {
	var data *llrb.LLRB
	switch cf {
	case engine_util.CfDefault:
		data = mr.inner.CfDefault
	case engine_util.CfLock:
		data = mr.inner.CfLock
	case engine_util.CfWrite:
		data = mr.inner.CfWrite
	default:
		return nil
	}

	min := data.Min()
	if min == nil {
		return &memIter{data, memItem{nil, nil}}
	}
	return &memIter{data, min.(memItem)}
}

func (r *memReader) Close() {}

type memIter struct {
	data *llrb.LLRB
	item memItem
}

func (it *memIter) Item() engine_util.DBItem {
	return it.item
}
func (it *memIter) Valid() bool {
	return it.item.key != nil
}
func (it *memIter) Next() {
	first := true
	oldItem := it.item
	it.item = memItem{nil, nil}
	it.data.AscendGreaterOrEqual(oldItem, func(item llrb.Item) bool {
		// Skip the first item, which will be it.item
		if first {
			first = false
			return true
		}

		it.item = item.(memItem)
		return false
	})
}
func (it *memIter) Seek(key []byte) {
	it.item = memItem{nil, nil}
	it.data.AscendGreaterOrEqual(memItem{key, nil}, func(item llrb.Item) bool {
		it.item = item.(memItem)

		return false
	})
}

func (it *memIter) Close() {}

type memItem struct {
	key   []byte
	value []byte
}

func (it memItem) Key() []byte {
	return it.key
}
func (it memItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it memItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it memItem) ValueSize() int {
	return len(it.value)
}
func (it memItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}

func (it memItem) Less(than llrb.Item) bool {
	other := than.(memItem)
	return bytes.Compare(it.key, other.key) < 0
}
