package inner_server

import (
	"context"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"github.com/pingcap/errors"
)

type MockRaftInnerServer struct {
	engines    *engine_util.Engines
	raftConfig *config.Config
	storeMeta  metapb.Store

	node        *raftstore.Node
	snapManager *snap.SnapManager
	raftRouter  *raftstore.RaftstoreRouter
	batchSystem *raftstore.RaftBatchSystem
	pdWorker    *worker.Worker
}

func (ris *MockRaftInnerServer) Write(ctx kvrpcpb.Context, batch []Modify) error {
	var reqs []*raft_cmdpb.Request
	for _, m := range batch {
		switch m.Type {
		case ModifyTypePut:
			put := m.Data.(Put)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutRequest{
					Cf:    put.Cf,
					Key:   put.Key,
					Value: put.Value,
				}})
		case ModifyTypeDelete:
			delete := m.Data.(Delete)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteRequest{
					Cf:  delete.Cf,
					Key: delete.Key,
				}})
		}
	}

	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header:   header,
		Requests: reqs,
	}
	cb := message.NewCallback()
	if err := ris.raftRouter.SendRaftCommand(request, cb); err != nil {
		return err
	}
	cb.Wg.Wait()
	return ris.checkResponse(cb.Resp, len(reqs))
}

func (ris *MockRaftInnerServer) checkResponse(resp *raft_cmdpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return errors.Errorf(resp.Header.Error.String())
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

func (ris *MockRaftInnerServer) Reader(ctx kvrpcpb.Context) (dbreader.DBReader, error) {
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raft_cmdpb.Request{&raft_cmdpb.Request{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapRequest{},
		}},
	}
	cb := message.NewCallback()
	if err := ris.raftRouter.SendRaftCommand(request, cb); err != nil {
		return nil, err
	}
	cb.Wg.Wait()

	return dbreader.NewRegionReader(cb.RegionSnap.Txn, cb.RegionSnap.Region), nil
}

func (ris *MockRaftInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		ris.raftRouter.SendRaftMessage(msg)
	}
}

func (ris *MockRaftInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			ris.raftRouter.SendRaftMessage(msg)
		}
	}
}

func (ris *MockRaftInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func NewMockRaftInnerServer(engines *engine_util.Engines, raftConfig *config.Config) *MockRaftInnerServer {
	return &MockRaftInnerServer{engines: engines, raftConfig: raftConfig}
}

func (ris *MockRaftInnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	ris.pdWorker = worker.NewWorker("pd-worker", &wg)

	cfg := ris.raftConfig
	router, batchSystem := raftstore.CreateRaftBatchSystem(cfg)

	ris.raftRouter = raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	ris.snapManager = snap.NewSnapManager(cfg.SnapPath)
	ris.batchSystem = batchSystem
}

func (ris *MockRaftInnerServer) GetRaftstoreRouter() *raftstore.RaftstoreRouter {
	return ris.raftRouter
}

func (ris *MockRaftInnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *MockRaftInnerServer) Start(pdClient pd.Client, trans *MockTransport) error {
	ris.node = raftstore.NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient)

	err := ris.node.Start(context.TODO(), ris.engines, trans, ris.snapManager, ris.pdWorker, ris.raftRouter)
	if err != nil {
		return err
	}
	return nil
}

func (ris *MockRaftInnerServer) Stop() error {
	ris.node.Stop()
	if err := ris.engines.Raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}
