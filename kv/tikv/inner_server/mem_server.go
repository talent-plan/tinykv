package inner_server

import (
	"github.com/coocood/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

// MemInnerServer is a simple inner server backed by memory for testing. Data is not written to disk, nor sent to other
// nodes. It is intended for testing only.
type MemInnerServer struct {
	Data map[byte][]byte
}

func NewMemInnerServer() *MemInnerServer {
	return &MemInnerServer{
		Data: make(map[byte][]byte),
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
			is.Data[data.Key[0]] = data.Value
		case Delete:
			delete(is.Data, data.Key[0])
		}
	}

	return nil
}

// memReader is a DBReader which reads from a MemInnerServer.
type memReader struct {
	inner *MemInnerServer
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	return mr.inner.Data[key[0]], nil
}

func (mr *memReader) IterCF(cf string) engine_util.DBIterator {
	var keys []byte
	for k := range mr.inner.Data {
		keys = append(keys, k)
	}
	return &memIter{mr.inner, keys, 0}
}

type memIter struct {
	inner    *MemInnerServer
	keys     []byte
	position int
}

func (it *memIter) Item() engine_util.DBItem {
	key := it.keys[it.position]
	value := it.inner.Data[key]
	return &memItem{
		[]byte{key}, value,
	}
}
func (it *memIter) Valid() bool {
	return it.position < len(it.keys)
}
func (it *memIter) Next() {
	it.position++
}
func (it *memIter) Seek(key []byte) {
	for i, k := range it.keys {
		if k == key[0] {
			it.position = i
			return
		}
	}

	// TODO this behaviour does not match badger. It should seek to "the next smallest key greater than provided", current
	// behaviour leave the iterator invalid.
	it.position = len(it.keys)
}

type memItem struct {
	key   []byte
	value []byte
}

func (it *memItem) Key() []byte {
	return it.key
}
func (it *memItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it *memItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it *memItem) ValueSize() int {
	return len(it.value)
}
func (it *memItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}
