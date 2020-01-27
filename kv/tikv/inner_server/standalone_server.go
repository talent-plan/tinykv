package inner_server

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

type StandAloneInnerServer struct {
	db *badger.DB
}

func NewStandAloneInnerServer(db *badger.DB) *StandAloneInnerServer {
	return &StandAloneInnerServer{
		db: db,
	}
}

func (is *StandAloneInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

func (is *StandAloneInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func (is *StandAloneInnerServer) Setup(pdClient pd.Client) {}

func (is *StandAloneInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *StandAloneInnerServer) Stop() error {
	return is.db.Close()
}

func (is *StandAloneInnerServer) Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error) {
	return nil, nil
}

func (is *StandAloneInnerServer) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	return nil
}
