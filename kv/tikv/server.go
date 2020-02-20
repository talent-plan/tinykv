package tikv

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/juju/errors"

	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/commands"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
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

const requestMaxSize = 6 * 1024 * 1024

func (svr *Server) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
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
	return getResponse(&resp)
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	cmd := commands.NewScan(req)
	resp := <-svr.scheduler.Run(&cmd)
	return scanResponse(&resp)
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	cmd := commands.NewPrewrite(req)
	resp := <-svr.scheduler.Run(&cmd)
	return prewriteResponse(&resp)
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	cmd := commands.NewCommit(req)
	resp := <-svr.scheduler.Run(&cmd)
	return commitResponse(&resp)
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	cmd := commands.NewCheckTxnStatus(req)
	resp := <-svr.scheduler.Run(&cmd)
	return checkTxnStatusResponse(&resp)
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	cmd := commands.NewRollback(req)
	resp := <-svr.scheduler.Run(&cmd)
	return rollbackResponse(&resp)
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, nil
}

// Raw API.
func (svr *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	cmd := commands.NewRawGet(req)
	resp := <-svr.scheduler.Run(&cmd)
	return rawGetResponse(&resp)
}

func (svr *Server) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	cmd := commands.NewRawPut(req)
	resp := <-svr.scheduler.Run(&cmd)
	return rawPutResponse(&resp)
}

func (svr *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	cmd := commands.NewRawDelete(req)
	resp := <-svr.scheduler.Run(&cmd)
	return rawDeleteResponse(&resp)
}

func (svr *Server) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	cmd := commands.NewRawScan(req)
	resp := <-svr.scheduler.Run(&cmd)
	return rawScanResponse(&resp)
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

func getResponse(sr *interfaces.SchedResult) (*kvrpcpb.GetResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.GetResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func prewriteResponse(sr *interfaces.SchedResult) (*kvrpcpb.PrewriteResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.PrewriteResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func scanResponse(sr *interfaces.SchedResult) (*kvrpcpb.ScanResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.ScanResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func checkTxnStatusResponse(sr *interfaces.SchedResult) (*kvrpcpb.CheckTxnStatusResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.CheckTxnStatusResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func commitResponse(sr *interfaces.SchedResult) (*kvrpcpb.CommitResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.CommitResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func rollbackResponse(sr *interfaces.SchedResult) (*kvrpcpb.BatchRollbackResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.BatchRollbackResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func resolveLockResponse(sr *interfaces.SchedResult) (*kvrpcpb.ResolveLockResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.ResolveLockResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func rawGetResponse(sr *interfaces.SchedResult) (*kvrpcpb.RawGetResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.RawGetResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func rawPutResponse(sr *interfaces.SchedResult) (*kvrpcpb.RawPutResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.RawPutResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func rawDeleteResponse(sr *interfaces.SchedResult) (*kvrpcpb.RawDeleteResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.RawDeleteResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func rawScanResponse(sr *interfaces.SchedResult) (*kvrpcpb.RawScanResponse, error) {
	if sr.Err != nil {
		return nil, sr.Err
	}
	if sr.Response == nil {
		return nil, nil
	}
	resp, ok := sr.Response.(*kvrpcpb.RawScanResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
