package tikv

import "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

// RespResult is a 'generic' result type for responses. It is used to return a Response/error pair over channels where
// we can't use Go's multiple return values.
type RespResult struct {
	Response interface{}
	Err      error
}

func RespOk(resp interface{}) RespResult {
	return RespResult{
		Response: resp,
		Err:      nil,
	}
}

func RespErr(err error) RespResult {
	return RespResult{
		Response: nil,
		Err:      err,
	}
}

func (rr *RespResult) getResponse() *kvrpcpb.GetResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.GetResponse)
}
func (rr *RespResult) prewriteResponse() *kvrpcpb.PrewriteResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.PrewriteResponse)
}
func (rr *RespResult) scan() *kvrpcpb.ScanResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.ScanResponse)
}
func (rr *RespResult) checkTxnStatus() *kvrpcpb.CheckTxnStatusResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.CheckTxnStatusResponse)
}
func (rr *RespResult) commit() *kvrpcpb.CommitResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.CommitResponse)
}
func (rr *RespResult) cleanup() *kvrpcpb.CleanupResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.CleanupResponse)
}
func (rr *RespResult) batchRollback() *kvrpcpb.BatchRollbackResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.BatchRollbackResponse)
}
func (rr *RespResult) scanLock() *kvrpcpb.ScanLockResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.ScanLockResponse)
}
func (rr *RespResult) resolveLock() *kvrpcpb.ResolveLockResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.ResolveLockResponse)
}
func (rr *RespResult) rawGetResponse() *kvrpcpb.RawGetResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.RawGetResponse)
}
func (rr *RespResult) rawPutResponse() *kvrpcpb.RawPutResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.RawPutResponse)
}
func (rr *RespResult) rawDeleteResponse() *kvrpcpb.RawDeleteResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.RawDeleteResponse)
}
func (rr *RespResult) rawScanResponse() *kvrpcpb.RawScanResponse {
	if rr.Response == nil {
		return nil
	}
	return rr.Response.(*kvrpcpb.RawScanResponse)
}
