package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/rowcodec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

var _ tikvpb.TikvServer = new(Server)

type Server struct {
	// mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   InnerServer
	wg            sync.WaitGroup
	refCount      int32
	stopped       int32
}

type InnerServer interface {
	Setup(pdClient pd.Client)
	Start(pdClient pd.Client) error
	Stop() error
	// TODO:
	// Put(...)
	// Delete(...)
	// Snapshot(...) used for get and scan
	Raft(stream tikvpb.Tikv_RaftServer) error
	BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

func NewServer(rm RegionManager, innerServer InnerServer) *Server {
	return &Server{
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

// type requestCtx struct {
// 	svr       *Server
// 	regCtx    *regionCtx
// 	regErr    *errorpb.Error
// 	buf       []byte
// 	reader    *dbreader.DBReader
// 	method    string
// 	startTime time.Time
// 	rpcCtx    *kvrpcpb.Context
// }

// func newRequestCtx(svr *Server, ctx *kvrpcpb.Context, method string) (*requestCtx, error) {
// 	atomic.AddInt32(&svr.refCount, 1)
// 	if atomic.LoadInt32(&svr.stopped) > 0 {
// 		atomic.AddInt32(&svr.refCount, -1)
// 		return nil, ErrRetryable("server is closed")
// 	}
// 	req := &requestCtx{
// 		svr:       svr,
// 		method:    method,
// 		startTime: time.Now(),
// 		rpcCtx:    ctx,
// 	}
// 	req.regCtx, req.regErr = svr.regionManager.GetRegionFromCtx(ctx)
// 	return req, nil
// }

// // For read-only requests that doesn't acquire latches, this function must be called after all locks has been checked.
// func (req *requestCtx) getDBReader() *dbreader.DBReader {
// 	if req.reader == nil {
// 		mvccStore := req.svr.mvccStore
// 		txn := mvccStore.db.NewTransaction(false)
// 		safePoint := atomic.LoadUint64(&mvccStore.safePoint.timestamp)
// 		req.reader = dbreader.NewDBReader(req.regCtx.startKey, req.regCtx.endKey, txn, safePoint)
// 	}
// 	return req.reader
// }

// func (req *requestCtx) finish() {
// 	atomic.AddInt32(&req.svr.refCount, -1)
// 	if req.reader != nil {
// 		req.reader.Close()
// 	}
// 	if req.regCtx != nil {
// 		req.regCtx.refCount.Done()
// 	}
// }

func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvGet")
	// if err != nil {
	// 	return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.GetResponse{RegionError: reqCtx.regErr}, nil
	// }
	// err = svr.mvccStore.CheckKeysLock(req.GetVersion(), req.Key)
	// if err != nil {
	// 	return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	// }
	// reader := reqCtx.getDBReader()
	// val, err := reader.Get(req.Key, req.GetVersion())
	// if err != nil {
	// 	return &kvrpcpb.GetResponse{
	// 		Error: convertToKeyError(err),
	// 	}, nil
	// }
	// if rowcodec.IsRowKey(req.Key) {
	// 	val, err = rowcodec.RowToOldRow(val, nil)
	// } else {
	// 	val = safeCopy(val)
	// }
	// return &kvrpcpb.GetResponse{
	// 	Value: val,
	// }, nil
	return nil, nil
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// 	reqCtx, err := newRequestCtx(svr, req.Context, "KvScan")
	// 	if err != nil {
	// 		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	// 	}
	// 	defer reqCtx.finish()
	// 	if reqCtx.regErr != nil {
	// 		return &kvrpcpb.ScanResponse{RegionError: reqCtx.regErr}, nil
	// 	}
	// 	if !isMvccRegion(reqCtx.regCtx) {
	// 		return &kvrpcpb.ScanResponse{}, nil
	// 	}
	// 	startKey := req.GetStartKey()
	// 	endKey := reqCtx.regCtx.rawEndKey()
	// 	err = svr.mvccStore.CheckRangeLock(req.GetVersion(), startKey, endKey)
	// 	if err != nil {
	// 		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	// 	}
	// 	var scanProc = &kvScanProcessor{}
	// 	reader := reqCtx.getDBReader()
	// 	err = reader.Scan(startKey, endKey, int(req.GetLimit()), req.GetVersion(), scanProc)
	// 	if err != nil {
	// 		scanProc.pairs = append(scanProc.pairs[:0], &kvrpcpb.KvPair{
	// 			Error: convertToKeyError(err),
	// 		})
	// 	}
	// 	return &kvrpcpb.ScanResponse{
	// 		Pairs: scanProc.pairs,
	// 	}, nil
	return nil, nil
}

type kvScanProcessor struct {
	skipVal bool
	buf     []byte
	pairs   []*kvrpcpb.KvPair
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

func (p *kvScanProcessor) SkipValue() bool {
	return p.skipVal
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "TxnHeartBeat")
	// if err != nil {
	// 	return &kvrpcpb.CheckTxnStatusResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.CheckTxnStatusResponse{RegionError: reqCtx.regErr}, nil
	// }
	// lockTTL, commitTS, action, err := svr.mvccStore.CheckTxnStatus(reqCtx, req)
	// resp := &kvrpcpb.CheckTxnStatusResponse{LockTtl: lockTTL, CommitVersion: commitTS, Action: action}
	// resp.Error, resp.RegionError = convertToPBError(err)
	// return resp, nil
	return nil, nil
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvPrewrite")
	// if err != nil {
	// 	return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.PrewriteResponse{RegionError: reqCtx.regErr}, nil
	// }
	// err = svr.mvccStore.Prewrite(reqCtx, req)
	// resp := &kvrpcpb.PrewriteResponse{}
	// resp.Errors, resp.RegionError = convertToPBErrors(err)
	// return resp, nil
	return nil, nil
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvCommit")
	// if err != nil {
	// 	return &kvrpcpb.CommitResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.CommitResponse{RegionError: reqCtx.regErr}, nil
	// }
	// resp := new(kvrpcpb.CommitResponse)
	// err = svr.mvccStore.Commit(reqCtx, req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	// if err != nil {
	// 	resp.Error, resp.RegionError = convertToPBError(err)
	// }
	// return resp, nil
	return nil, nil
}

func (svr *Server) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvCleanup")
	// if err != nil {
	// 	return &kvrpcpb.CleanupResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.CleanupResponse{RegionError: reqCtx.regErr}, nil
	// }
	// err = svr.mvccStore.Cleanup(reqCtx, req.Key, req.StartVersion, req.CurrentTs)
	// resp := new(kvrpcpb.CleanupResponse)
	// if committed, ok := err.(ErrAlreadyCommitted); ok {
	// 	resp.CommitVersion = uint64(committed)
	// } else if err != nil {
	// 	log.Error(err)
	// 	resp.Error, resp.RegionError = convertToPBError(err)
	// }
	// return resp, nil
	return nil, nil
}

func (svr *Server) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchGet")
	// if err != nil {
	// 	return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.BatchGetResponse{RegionError: reqCtx.regErr}, nil
	// }
	// err = svr.mvccStore.CheckKeysLock(req.GetVersion(), req.Keys...)
	// if err != nil {
	// 	return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	// }
	// pairs := make([]*kvrpcpb.KvPair, 0, len(req.Keys))
	// var buf []byte
	// batchGetFunc := func(key, value []byte, err error) {
	// 	if len(value) != 0 {
	// 		if rowcodec.IsRowKey(key) && err == nil {
	// 			buf, err = rowcodec.RowToOldRow(value, buf)
	// 			value = buf
	// 		}
	// 		pairs = append(pairs, &kvrpcpb.KvPair{
	// 			Key:   safeCopy(key),
	// 			Value: safeCopy(value),
	// 			Error: convertToKeyError(err),
	// 		})
	// 	}
	// }
	// reqCtx.getDBReader().BatchGet(req.Keys, req.GetVersion(), batchGetFunc)
	// return &kvrpcpb.BatchGetResponse{
	// 	Pairs: pairs,
	// }, nil
	return nil, nil
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchRollback")
	// if err != nil {
	// 	return &kvrpcpb.BatchRollbackResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.BatchRollbackResponse{RegionError: reqCtx.regErr}, nil
	// }
	// resp := new(kvrpcpb.BatchRollbackResponse)
	// err = svr.mvccStore.Rollback(reqCtx, req.Keys, req.StartVersion)
	// resp.Error, resp.RegionError = convertToPBError(err)
	// return resp, nil
	return nil, nil
}

func (svr *Server) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvScanLock")
	// if err != nil {
	// 	return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.ScanLockResponse{RegionError: reqCtx.regErr}, nil
	// }
	// log.Debug("kv scan lock")
	// if !isMvccRegion(reqCtx.regCtx) {
	// 	return &kvrpcpb.ScanLockResponse{}, nil
	// }
	// locks, err := svr.mvccStore.ScanLock(reqCtx, req.MaxVersion)
	// return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err), Locks: locks}, nil
	return nil, nil
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// reqCtx, err := newRequestCtx(svr, req.Context, "KvResolveLock")
	// if err != nil {
	// 	return &kvrpcpb.ResolveLockResponse{Error: convertToKeyError(err)}, nil
	// }
	// defer reqCtx.finish()
	// if reqCtx.regErr != nil {
	// 	return &kvrpcpb.ResolveLockResponse{RegionError: reqCtx.regErr}, nil
	// }
	// if !isMvccRegion(reqCtx.regCtx) {
	// 	return &kvrpcpb.ResolveLockResponse{}, nil
	// }
	// resp := &kvrpcpb.ResolveLockResponse{}
	// if len(req.TxnInfos) > 0 {
	// 	for _, txnInfo := range req.TxnInfos {
	// 		log.Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, txnInfo.Txn)
	// 		err := svr.mvccStore.ResolveLock(reqCtx, txnInfo.Txn, txnInfo.Status)
	// 		if err != nil {
	// 			resp.Error, resp.RegionError = convertToPBError(err)
	// 			break
	// 		}
	// 	}
	// } else {
	// 	log.Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, req.StartVersion)
	// 	err := svr.mvccStore.ResolveLock(reqCtx, req.StartVersion, req.CommitVersion)
	// 	resp.Error, resp.RegionError = convertToPBError(err)
	// }
	// return resp, nil
	return nil, nil
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
