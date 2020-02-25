package tikv

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

var _ tikvpb.TikvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	innerServer interfaces.InnerServer
	scheduler   interfaces.Scheduler
	refCount    int32
	stopped     int32
}

func NewServer(innerServer interfaces.InnerServer, scheduler interfaces.Scheduler) *Server {
	return &Server{
		innerServer: innerServer,
		scheduler:   scheduler,
	}
}

func (svr *Server) Stop() error {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			svr.scheduler.Stop()
			return svr.innerServer.Stop()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// The below functions are Server's gRPC API (implements TikvServer).

// Transactional API.
func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	cmd := commands.NewGet(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.GetResponse), resp.Err
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	cmd := commands.NewScan(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.ScanResponse), resp.Err
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	cmd := commands.NewPrewrite(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.PrewriteResponse), resp.Err
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	cmd := commands.NewCommit(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.CommitResponse), resp.Err
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	cmd := commands.NewCheckTxnStatus(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.CheckTxnStatusResponse), resp.Err
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	cmd := commands.NewRollback(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.BatchRollbackResponse), resp.Err
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	cmd := commands.NewResolveLock(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.ResolveLockResponse), resp.Err
}

// Raw API.
func (svr *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	cmd := commands.NewRawGet(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.RawGetResponse), resp.Err
}

func (svr *Server) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	cmd := commands.NewRawPut(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.RawPutResponse), resp.Err
}

func (svr *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	cmd := commands.NewRawDelete(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.RawDeleteResponse), resp.Err
}

func (svr *Server) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	cmd := commands.NewRawScan(req)
	resp := <-svr.scheduler.Run(&cmd)
	return resp.Response.(*kvrpcpb.RawScanResponse), resp.Err
}

// Raft commands (tikv <-> tikv); these are trivially forwarded to innerServer.
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}

func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
}

// SQL push down commands.
func (svr *Server) Coprocessor(ctx context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}
