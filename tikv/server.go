package tikv

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/ngaut/unistore/util/lockwaiter"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	deadlockPb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tikvpb.TikvServer = new(Server)

type Server struct {
	mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   InnerServer
	wg            sync.WaitGroup
	refCount      int32
	stopped       int32
}

func NewServer(rm RegionManager, store *MVCCStore, innerServer InnerServer) *Server {
	return &Server{
		mvccStore:     store,
		regionManager: rm,
		innerServer:   innerServer,
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

func (svr *Server) Stop() {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

type requestCtx struct {
	svr       *Server
	regCtx    *regionCtx
	regErr    *errorpb.Error
	buf       []byte
	reader    *dbreader.DBReader
	method    string
	startTime time.Time
	rpcCtx    *kvrpcpb.Context
}

func newRequestCtx(svr *Server, ctx *kvrpcpb.Context, method string) (*requestCtx, error) {
	atomic.AddInt32(&svr.refCount, 1)
	if atomic.LoadInt32(&svr.stopped) > 0 {
		atomic.AddInt32(&svr.refCount, -1)
		return nil, ErrRetryable("server is closed")
	}
	req := &requestCtx{
		svr:       svr,
		method:    method,
		startTime: time.Now(),
		rpcCtx:    ctx,
	}
	req.regCtx, req.regErr = svr.regionManager.GetRegionFromCtx(ctx)
	return req, nil
}

// For read-only requests that doesn't acquire latches, this function must be called after all locks has been checked.
func (req *requestCtx) getDBReader() *dbreader.DBReader {
	if req.reader == nil {
		mvccStore := req.svr.mvccStore
		txn := mvccStore.db.NewTransaction(false)
		safePoint := atomic.LoadUint64(&mvccStore.safePoint.timestamp)
		req.reader = dbreader.NewDBReader(req.regCtx.startKey, req.regCtx.endKey, txn, safePoint)
	}
	return req.reader
}

func (req *requestCtx) finish() {
	atomic.AddInt32(&req.svr.refCount, -1)
	if req.reader != nil {
		req.reader.Close()
	}
	if req.regCtx != nil {
		req.regCtx.refCount.Done()
	}
}

func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGet")
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.GetResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.CheckKeysLock(req.GetVersion(), req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	reader := reqCtx.getDBReader()
	val, err := reader.Get(req.Key, req.GetVersion())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}, nil
	}
	if rowcodec.IsRowKey(req.Key) {
		val, err = rowcodec.RowToOldRow(val, nil)
	} else {
		val = safeCopy(val)
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}, nil
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScan")
	if err != nil {
		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanResponse{RegionError: reqCtx.regErr}, nil
	}
	if !isMvccRegion(reqCtx.regCtx) {
		return &kvrpcpb.ScanResponse{}, nil
	}
	startKey := req.GetStartKey()
	endKey := reqCtx.regCtx.rawEndKey()
	err = svr.mvccStore.CheckRangeLock(req.GetVersion(), startKey, endKey)
	if err != nil {
		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	var scanProc = &kvScanProcessor{}
	reader := reqCtx.getDBReader()
	err = reader.Scan(startKey, endKey, int(req.GetLimit()), req.GetVersion(), scanProc)
	if err != nil {
		scanProc.pairs = append(scanProc.pairs[:0], &kvrpcpb.KvPair{
			Error: convertToKeyError(err),
		})
	}
	return &kvrpcpb.ScanResponse{
		Pairs: scanProc.pairs,
	}, nil
}

type kvScanProcessor struct {
	skipVal
	buf   []byte
	pairs []*kvrpcpb.KvPair
}

func (p *kvScanProcessor) Process(key, value []byte) (err error) {
	if rowcodec.IsRowKey(key) {
		p.buf, err = rowcodec.RowToOldRow(value, p.buf)
		if err != nil {
			return err
		}
		value = p.buf
	}
	p.pairs = append(p.pairs, &kvrpcpb.KvPair{
		Key:   safeCopy(key),
		Value: safeCopy(value),
	})
	return nil
}

func (svr *Server) KvPessimisticLock(ctx context.Context, req *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticLock")
	if err != nil {
		return &kvrpcpb.PessimisticLockResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticLockResponse{RegionError: reqCtx.regErr}, nil
	}
	waiter, err := svr.mvccStore.PessimisticLock(reqCtx, req)
	resp := &kvrpcpb.PessimisticLockResponse{}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	if waiter == nil {
		return resp, nil
	}
	result := waiter.Wait()
	svr.mvccStore.DeadlockDetectCli.CleanUpWaitFor(req.StartVersion, waiter.LockTS, waiter.KeyHash)
	if result.Position == lockwaiter.WaitTimeout {
		svr.mvccStore.lockWaiterManager.CleanUp(waiter)
		return resp, nil
	} else if result.DeadlockResp != nil {
		log.Errorf("deadlock found for entry=%v", result.DeadlockResp.Entry)
		errLocked := err.(*ErrLocked)
		deadlockErr := &ErrDeadlock{
			LockKey:         errLocked.Key,
			LockTS:          errLocked.StartTS,
			DeadlockKeyHash: result.DeadlockResp.DeadlockKeyHash,
		}
		resp.Errors, resp.RegionError = convertToPBErrors(deadlockErr)
		return resp, nil
	}
	if result.Position > 0 {
		// Sleep a little so the transaction in lower position will more likely get the lock.
		time.Sleep(time.Millisecond * 3)
	}
	conflictCommitTS := result.CommitTS
	if conflictCommitTS < req.GetForUpdateTs() {
		// The key is rollbacked, we don't have the exact commitTS, but we can use the server's latest.
		conflictCommitTS = svr.mvccStore.getLatestTS()
	}
	err = &ErrConflict{
		StartTS:          req.GetForUpdateTs(),
		ConflictTS:       waiter.LockTS,
		ConflictCommitTS: conflictCommitTS,
	}
	resp.Errors, _ = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KVPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticRollback")
	if err != nil {
		return &kvrpcpb.PessimisticRollbackResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.PessimisticRollback(reqCtx, req)
	resp := &kvrpcpb.PessimisticRollbackResponse{}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KvTxnHeartBeat(ctx context.Context, req *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "TxnHeartBeat")
	if err != nil {
		return &kvrpcpb.TxnHeartBeatResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.TxnHeartBeatResponse{RegionError: reqCtx.regErr}, nil
	}
	lockTTL, err := svr.mvccStore.TxnHeartBeat(reqCtx, req)
	resp := &kvrpcpb.TxnHeartBeatResponse{LockTtl: lockTTL}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "TxnHeartBeat")
	if err != nil {
		return &kvrpcpb.CheckTxnStatusResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CheckTxnStatusResponse{RegionError: reqCtx.regErr}, nil
	}
	lockTTL, commitTS, action, err := svr.mvccStore.CheckTxnStatus(reqCtx, req)
	resp := &kvrpcpb.CheckTxnStatusResponse{LockTtl: lockTTL, CommitVersion: commitTS, Action: action}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvPrewrite")
	if err != nil {
		return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PrewriteResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Prewrite(reqCtx, req)
	resp := &kvrpcpb.PrewriteResponse{}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCommit")
	if err != nil {
		return &kvrpcpb.CommitResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CommitResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.CommitResponse)
	err = svr.mvccStore.Commit(reqCtx, req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	// TODO
	return &kvrpcpb.ImportResponse{}, nil
}

func (svr *Server) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCleanup")
	if err != nil {
		return &kvrpcpb.CleanupResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CleanupResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Cleanup(reqCtx, req.Key, req.StartVersion, req.CurrentTs)
	resp := new(kvrpcpb.CleanupResponse)
	if committed, ok := err.(ErrAlreadyCommitted); ok {
		resp.CommitVersion = uint64(committed)
	} else if err != nil {
		log.Error(err)
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchGet")
	if err != nil {
		return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchGetResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.CheckKeysLock(req.GetVersion(), req.Keys...)
	if err != nil {
		return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	pairs := make([]*kvrpcpb.KvPair, 0, len(req.Keys))
	var buf []byte
	batchGetFunc := func(key, value []byte, err error) {
		if len(value) != 0 {
			if rowcodec.IsRowKey(key) && err == nil {
				buf, err = rowcodec.RowToOldRow(value, buf)
				value = buf
			}
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:   safeCopy(key),
				Value: safeCopy(value),
				Error: convertToKeyError(err),
			})
		}
	}
	reqCtx.getDBReader().BatchGet(req.Keys, req.GetVersion(), batchGetFunc)
	return &kvrpcpb.BatchGetResponse{
		Pairs: pairs,
	}, nil
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchRollback")
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.BatchRollbackResponse)
	err = svr.mvccStore.Rollback(reqCtx, req.Keys, req.StartVersion)
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScanLock")
	if err != nil {
		return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanLockResponse{RegionError: reqCtx.regErr}, nil
	}
	log.Debug("kv scan lock")
	if !isMvccRegion(reqCtx.regCtx) {
		return &kvrpcpb.ScanLockResponse{}, nil
	}
	locks, err := svr.mvccStore.ScanLock(reqCtx, req.MaxVersion)
	return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err), Locks: locks}, nil
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvResolveLock")
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ResolveLockResponse{RegionError: reqCtx.regErr}, nil
	}
	if !isMvccRegion(reqCtx.regCtx) {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	resp := &kvrpcpb.ResolveLockResponse{}
	if len(req.TxnInfos) > 0 {
		for _, txnInfo := range req.TxnInfos {
			log.Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, txnInfo.Txn)
			err := svr.mvccStore.ResolveLock(reqCtx, txnInfo.Txn, txnInfo.Status)
			if err != nil {
				resp.Error, resp.RegionError = convertToPBError(err)
				break
			}
		}
	} else {
		log.Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, req.StartVersion)
		err := svr.mvccStore.ResolveLock(reqCtx, req.StartVersion, req.CommitVersion)
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGC")
	if err != nil {
		return &kvrpcpb.GCResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.GCResponse{RegionError: reqCtx.regErr}, nil
	}
	log.Debug("kv GC safePoint:", extractPhysicalTime(req.SafePoint))
	if !isMvccRegion(reqCtx.regCtx) {
		return &kvrpcpb.GCResponse{}, nil
	}
	err = svr.mvccStore.GC(reqCtx, req.SafePoint)
	return &kvrpcpb.GCResponse{Error: convertToKeyError(err)}, nil
}

func (svr *Server) KvDeleteRange(ctx context.Context, req *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvDeleteRange")
	if err != nil {
		return &kvrpcpb.DeleteRangeResponse{Error: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.DeleteRangeResponse{RegionError: reqCtx.regErr}, nil
	}
	if !isMvccRegion(reqCtx.regCtx) {
		return &kvrpcpb.DeleteRangeResponse{}, nil
	}
	err = svr.mvccStore.dbWriter.DeleteRange(req.StartKey, req.EndKey, reqCtx.regCtx)
	if err != nil {
		log.Error(err)
	}
	return &kvrpcpb.DeleteRangeResponse{}, nil
}

// RawKV commands.
func (svr *Server) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return &kvrpcpb.RawGetResponse{}, nil
}

func (svr *Server) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return &kvrpcpb.RawPutResponse{}, nil
}

func (svr *Server) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (svr *Server) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return &kvrpcpb.RawScanResponse{}, nil
}

func (svr *Server) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return &kvrpcpb.RawBatchDeleteResponse{}, nil
}

func (svr *Server) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return &kvrpcpb.RawBatchGetResponse{}, nil
}

func (svr *Server) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return &kvrpcpb.RawBatchPutResponse{}, nil
}

func (svr *Server) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return &kvrpcpb.RawBatchScanResponse{}, nil
}

func (svr *Server) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return &kvrpcpb.RawDeleteRangeResponse{}, nil
}

// SQL push down commands.
func (svr *Server) Coprocessor(ctx context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "Coprocessor")
	if err != nil {
		return &coprocessor.Response{OtherError: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &coprocessor.Response{RegionError: reqCtx.regErr}, nil
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return svr.handleCopDAGRequest(reqCtx, req), nil
	case kv.ReqTypeAnalyze:
		return svr.handleCopAnalyzeRequest(reqCtx, req), nil
	}
	return &coprocessor.Response{OtherError: fmt.Sprintf("unsupported request type %d", req.GetTp())}, nil
}

func (svr *Server) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	// TODO
	return nil
}

// Raft commands (tikv <-> tikv).
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}
func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
}

func (svr *Server) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return svr.innerServer.BatchRaft(stream)
}

// Region commands.
func (svr *Server) SplitRegion(ctx context.Context, req *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return svr.innerServer.SplitRegion(req), nil
}

func (svr *Server) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	// TODO:
	return &kvrpcpb.ReadIndexResponse{}, nil
}

// transaction debugger commands.
func (svr *Server) MvccGetByKey(ctx context.Context, req *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByKey")
	if err != nil {
		return &kvrpcpb.MvccGetByKeyResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByKeyResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByKeyResponse)
	mvccInfo, err := svr.mvccStore.MvccGetByKey(reqCtx, req.GetKey())
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	return resp, nil
}

func (svr *Server) MvccGetByStartTs(ctx context.Context, req *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByStartTs")
	if err != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByStartTsResponse)
	mvccInfo, key, err := svr.mvccStore.MvccGetByStartTs(reqCtx, req.StartTs)
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	resp.Key = key
	return resp, nil
}

func (svr *Server) UnsafeDestroyRange(ctx context.Context, req *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	start, end := req.GetStartKey(), req.GetEndKey()
	svr.mvccStore.DeleteFileInRange(start, end)
	return &kvrpcpb.UnsafeDestroyRangeResponse{}, nil
}

// deadlock detection related services
// GetWaitForEntries tries to get the waitFor entries
func (svr *Server) GetWaitForEntries(ctx context.Context,
	req *deadlockPb.WaitForEntriesRequest) (*deadlockPb.WaitForEntriesResponse, error) {
	// TODO
	return &deadlockPb.WaitForEntriesResponse{}, nil
}

// Detect will handle detection rpc from other nodes
func (svr *Server) Detect(stream deadlockPb.Deadlock_DetectServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !svr.mvccStore.DeadlockDetectSvr.IsLeader() {
			log.Warnf("detection requests received on non leader node")
			break
		}
		switch req.Tp {
		case deadlockPb.DeadlockRequestType_Detect:
			err := svr.mvccStore.DeadlockDetectSvr.Detector.Detect(req.Entry.Txn, req.Entry.WaitForTxn, req.Entry.KeyHash)
			if err != nil {
				resp := convertErrToResp(err, req.Entry.Txn,
					req.Entry.WaitForTxn, req.Entry.KeyHash)
				sendErr := stream.Send(resp)
				if sendErr != nil {
					log.Errorf("send deadlock response failed, error=%v", sendErr)
					break
				}
			}
		case deadlockPb.DeadlockRequestType_CleanUpWaitFor:
			svr.mvccStore.DeadlockDetectSvr.Detector.CleanUpWaitFor(req.Entry.Txn, req.Entry.WaitForTxn, req.Entry.KeyHash)
		case deadlockPb.DeadlockRequestType_CleanUp:
			svr.mvccStore.DeadlockDetectSvr.Detector.CleanUp(req.Entry.Txn)
		}
	}
	return nil
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}
	causeErr := errors.Cause(err)
	switch x := causeErr.(type) {
	case *ErrLocked:
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         x.Key,
				PrimaryLock: x.Primary,
				LockVersion: x.StartTS,
				LockTtl:     x.TTL,
			},
		}
	case ErrRetryable:
		return &kvrpcpb.KeyError{
			Retryable: x.Error(),
		}
	case *ErrKeyAlreadyExists:
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: x.Key,
			},
		}
	case *ErrConflict:
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:          x.StartTS,
				ConflictTs:       x.ConflictTS,
				ConflictCommitTs: x.ConflictCommitTS,
				Key:              x.Key,
			},
		}
	case *ErrDeadlock:
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockKey:         x.LockKey,
				LockTs:          x.LockTS,
				DeadlockKeyHash: x.DeadlockKeyHash,
			},
		}
	case *ErrCommitExpire:
		return &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           x.StartTs,
				AttemptedCommitTs: x.CommitTs,
				Key:               x.Key,
				MinCommitTs:       x.MinCommitTs,
			},
		}
	case *ErrTxnNotFound:
		return &kvrpcpb.KeyError{
			TxnNotFound: &kvrpcpb.TxnNotFound{
				StartTs:    x.StartTS,
				PrimaryKey: x.PrimaryKey,
			},
		}
	case *ErrCommitPessimisticLock:
		return &kvrpcpb.KeyError{
			Abort: x.Error(),
		}
	default:
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
}

func convertToPBError(err error) (*kvrpcpb.KeyError, *errorpb.Error) {
	if regErr := extractRegionError(err); regErr != nil {
		return nil, regErr
	}
	return convertToKeyError(err), nil
}

func convertToPBErrors(err error) ([]*kvrpcpb.KeyError, *errorpb.Error) {
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			return nil, regErr
		}
		return []*kvrpcpb.KeyError{convertToKeyError(err)}, nil
	}
	return nil, nil
}

func extractRegionError(err error) *errorpb.Error {
	if raftError, ok := err.(*raftstore.RaftError); ok {
		return raftError.RequestErr
	}
	return nil
}

func isMvccRegion(regCtx *regionCtx) bool {
	if len(regCtx.startKey) == 0 {
		return false
	}
	first := regCtx.startKey[0]
	return first == 't' || first == 'm'
}
