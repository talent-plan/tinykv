package raftstore

import (
	"context"

	copr "github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
)

type KVService struct {
	router        *RaftstoreRouter
	snapScheduler chan<- task
	// TODO: storage, endpoint
}

func NewKVService(router *RaftstoreRouter, snapScheduler chan<- task) *KVService {
	return &KVService{
		router:        router,
		snapScheduler: snapScheduler,
	}
}

func (s *KVService) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		s.router.SendRaftMessage(msg)
	}
}

func (s *KVService) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			s.router.SendRaftMessage(msg)
		}
	}
}

func (s *KVService) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	s.snapScheduler <- task{
		tp: taskTypeSnapRecv,
		data: recvSnapTask{
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

func (s *KVService) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	panic("unimplemented")
}

func (s *KVService) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	panic("unimplemented")
}

func (s *KVService) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	panic("unimplemented")
}

func (s *KVService) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	panic("unimplemented")
}

func (s *KVService) Coprocessor(context.Context, *copr.Request) (*copr.Response, error) {
	panic("unimplemented")
}

func (s *KVService) CoprocessorStream(*copr.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	panic("unimplemented")
}

func (s *KVService) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	panic("unimplemented")
}

func (s *KVService) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	panic("unimplemented")
}

func (s *KVService) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	panic("unimplemented")
}

func (s *KVService) BatchCommands(tikvpb.Tikv_BatchCommandsServer) error {
	panic("unimplemented")
}
