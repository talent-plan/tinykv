package tikv

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

var _ tikvpb.TikvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	innerServer interfaces.InnerServer
	scheduler   *exec.Scheduler
	refCount    int32
	stopped     int32
}

func NewServer(innerServer interfaces.InnerServer, scheduler *exec.Scheduler) *Server {
	return &Server{
		innerServer: innerServer,
		scheduler:   scheduler,
	}
}

// The below functions are Server's gRPC API (implements TikvServer).

// Transactional API.
func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	cmd := commands.NewGet(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.GetResponse), err
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	cmd := commands.NewScan(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.ScanResponse), err
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	cmd := commands.NewPrewrite(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.PrewriteResponse), err
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	cmd := commands.NewCommit(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.CommitResponse), err
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	cmd := commands.NewCheckTxnStatus(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.CheckTxnStatusResponse), err
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	cmd := commands.NewRollback(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.BatchRollbackResponse), err
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	cmd := commands.NewResolveLock(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.ResolveLockResponse), err
}

// Raw API.
func (svr *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	cmd := commands.NewRawGet(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.RawGetResponse), err
}

func (svr *Server) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	cmd := commands.NewRawPut(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.RawPutResponse), err
}

func (svr *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	cmd := commands.NewRawDelete(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.RawDeleteResponse), err
}

func (svr *Server) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	cmd := commands.NewRawScan(req)
	resp, err := svr.scheduler.Run(&cmd)
	return resp.(*kvrpcpb.RawScanResponse), err
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
