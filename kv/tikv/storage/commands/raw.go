package commands

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// RawGet implements the Command interface for raw get requests.
type RawGet struct {
	request  *kvrpcpb.RawGetRequest
	response kvrpcpb.RawGetResponse
}

func NewRawGet(request *kvrpcpb.RawGetRequest) RawGet {
	return RawGet{request, kvrpcpb.RawGetResponse{}}
}

func (rg *RawGet) BuildTxn(txn *kvstore.Txn) error {
	val, err := txn.Reader.GetCF(rg.request.Cf, rg.request.Key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			rg.response.NotFound = true
		} else {
			return err
		}
	} else {
		rg.response.Value = val
	}
	return nil
}

func (rg *RawGet) Context() *kvrpcpb.Context {
	return rg.request.Context
}

func (rg *RawGet) Response() (interface{}, error) {
	return &rg.response, nil
}

func (rg *RawGet) RegionError(err *errorpb.Error) interface{} {
	if err == nil {
		return nil
	}

	rg.response.RegionError = err
	return &rg.response
}
