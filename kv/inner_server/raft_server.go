package inner_server

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"github.com/pingcap/errors"
)

// RaftInnerServer is an InnerServer (see tikv/server.go) backed by a Raft node. It is part of a Raft network.
// By using Raft, reads and writes are consistent with other nodes in the TinyKV instance.
type RaftInnerServer struct {
	engines    *engine_util.Engines
	raftConfig *config.Config
	storeMeta  metapb.Store

	node          *raftstore.Node
	snapManager   *snap.SnapManager
	raftRouter    *raftstore.RaftstoreRouter
	batchSystem   *raftstore.RaftBatchSystem
	resolveWorker *worker.Worker
	snapWorker    *worker.Worker

	wg sync.WaitGroup
}

type RegionError struct {
	RequestErr *errorpb.Error
}

func (re *RegionError) Error() string {
	return re.RequestErr.String()
}

func (ris *RaftInnerServer) checkResponse(resp *raft_cmdpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return &RegionError{RequestErr: resp.Header.Error}
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

// NewRaftInnerServer creates a new inner server backed by a raftstore.
func NewRaftInnerServer(conf *config.Config) *RaftInnerServer {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	raftDB := engine_util.CreateDB("raft", conf)
	kvDB := engine_util.CreateDB("kv", conf)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)

	return &RaftInnerServer{engines: engines, raftConfig: conf}
}

func (ris *RaftInnerServer) Write(ctx *kvrpcpb.Context, batch []Modify) error {
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

	return ris.checkResponse(cb.WaitResp(), len(reqs))
}

func (ris *RaftInnerServer) Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error) {
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raft_cmdpb.Request{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapRequest{},
		}},
	}
	cb := message.NewCallback()
	if err := ris.raftRouter.SendRaftCommand(request, cb); err != nil {
		return nil, err
	}

	resp := cb.WaitResp()
	if err := ris.checkResponse(resp, 1); err != nil {
		if cb.Txn != nil {
			cb.Txn.Discard()
		}
		return nil, err
	}
	if cb.Txn == nil {
		panic("can not found region snap")
	}
	if len(resp.Responses) != 1 {
		panic("wrong response count for snap cmd")
	}
	return dbreader.NewRegionReader(cb.Txn, *resp.Responses[0].GetSnap().Region), nil
}

func (ris *RaftInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		ris.raftRouter.SendRaftMessage(msg)
	}
}

func (ris *RaftInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	ris.snapWorker.Sender() <- worker.Task{
		Tp: worker.TaskTypeSnapRecv,
		Data: recvSnapTask{
			stream: stream,
			callback: func(e error) {
				err = e
				close(done)
			},
		},
	}
	<-done
	return err
}

func (ris *RaftInnerServer) GetRaftstoreRouter() *raftstore.RaftstoreRouter {
	return ris.raftRouter
}

func (ris *RaftInnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *RaftInnerServer) Start(pdClient pd.Client) error {
	ris.resolveWorker = worker.NewWorker("resolver", &ris.wg)
	ris.snapWorker = worker.NewWorker("snap-worker", &ris.wg)

	cfg := ris.raftConfig
	router, batchSystem := raftstore.CreateRaftBatchSystem(cfg)

	ris.snapManager = snap.NewSnapManager(cfg.DBPath + "snap")
	ris.batchSystem = batchSystem
	ris.raftRouter = raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	ris.node = raftstore.NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient)

	resolveSender := ris.resolveWorker.Sender()
	raftClient := newRaftClient(cfg)
	trans := NewServerTransport(raftClient, resolveSender, ris.raftRouter, resolveSender)

	resolveRunner := newResolverRunner(pdClient)

	ris.resolveWorker.Start(resolveRunner)
	err := ris.node.Start(context.TODO(), ris.engines, trans, ris.snapManager, ris.raftRouter)
	if err != nil {
		return err
	}
	snapRunner := newSnapRunner(ris.snapManager, ris.raftConfig, ris.raftRouter)
	ris.snapWorker.Start(snapRunner)

	return nil
}

func (ris *RaftInnerServer) Stop() error {
	ris.snapWorker.Stop()
	ris.node.Stop()
	ris.resolveWorker.Stop()
	ris.wg.Wait()
	if err := ris.engines.Raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}
