package tikv

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

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

func (rr *RespResult) getResponse() (*kvrpcpb.GetResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.GetResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) prewriteResponse() (*kvrpcpb.PrewriteResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.PrewriteResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) scan() (*kvrpcpb.ScanResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.ScanResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) checkTxnStatus() (*kvrpcpb.CheckTxnStatusResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.CheckTxnStatusResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) commit() (*kvrpcpb.CommitResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.CommitResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) batchRollback() (*kvrpcpb.BatchRollbackResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.BatchRollbackResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) resolveLock() (*kvrpcpb.ResolveLockResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.ResolveLockResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) rawGetResponse() (*kvrpcpb.RawGetResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.RawGetResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) rawPutResponse() (*kvrpcpb.RawPutResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.RawPutResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) rawDeleteResponse() (*kvrpcpb.RawDeleteResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.RawDeleteResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
func (rr *RespResult) rawScanResponse() (*kvrpcpb.RawScanResponse, error) {
	if rr.Err != nil {
		return nil, rr.Err
	}
	if rr.Response == nil {
		return nil, nil
	}
	resp, ok := rr.Response.(*kvrpcpb.RawScanResponse)
	if ok {
		return resp, nil
	}
	return nil, errors.New("Unexpected type in response")
}
