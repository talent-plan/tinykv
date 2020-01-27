package inner_server

import (
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
	return nil
}

// memReader is a DBReader which reads from a MemInnerServer.
type memReader struct {
	inner *MemInnerServer
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	return mr.inner.Data[key[0]], nil
}

func (mr *memReader) IterCF(cf string) *engine_util.CFIterator {
	return nil
}
