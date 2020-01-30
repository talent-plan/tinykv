package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Get struct {
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{}
}

func (g *Get) BuildTxn(txn *kvstore.Txn) error {
	return nil
}

func (g *Get) Context() *kvrpcpb.Context {
	return nil
}

func (g *Get) Response() interface{} {
	return nil
}

func (g *Get) HandleError(err error) interface{} {
	return nil
}
