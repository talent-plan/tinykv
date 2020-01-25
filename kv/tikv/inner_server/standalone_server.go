package inner_server

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

type StandAlongInnerServer struct {
	db *badger.DB
}

func NewStandAlongInnerServer(db *badger.DB) *StandAlongInnerServer {
	return &StandAlongInnerServer{
		db: db,
	}
}

func (is *StandAlongInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

func (is *StandAlongInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return nil
}

func (is *StandAlongInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func (is *StandAlongInnerServer) SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	return &kvrpcpb.SplitRegionResponse{}
}

func (is *StandAlongInnerServer) Setup(pdClient pd.Client) {}

func (is *StandAlongInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *StandAlongInnerServer) Stop() error {
	return is.db.Close()
}

func (is *StandAlongInnerServer) Reader(ctx kvrpcpb.Context) (dbreader.DBReader, error) {
	return nil, nil
}

func (is *StandAlongInnerServer) Write(ctx kvrpcpb.Context, batch []Modify) error {
	return nil
}
