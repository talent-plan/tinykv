package standalone_server

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneInnerServer is an InnerServer for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneInnerServer struct {
	// Your Data Here (1).
}

func NewStandAloneInnerServer(conf *config.Config) *StandAloneInnerServer {
	// Your Code Here (1).
	return nil
}

func (is *StandAloneInnerServer) Start() error {
	// Your Code Here (1).
	return nil
}

func (is *StandAloneInnerServer) Stop() error {
	// Your Code Here (1).
	return nil
}

func (is *StandAloneInnerServer) Reader(ctx *kvrpcpb.Context) (inner_server.DBReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (is *StandAloneInnerServer) Write(ctx *kvrpcpb.Context, batch []inner_server.Modify) error {
	// Your Code Here (1).
	return nil
}
