package commands

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
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

// RawPut implements the Command interface for raw put requests.
type RawPut struct {
	request *kvrpcpb.RawPutRequest
}

func NewRawPut(request *kvrpcpb.RawPutRequest) RawPut {
	return RawPut{request}
}

func (rp *RawPut) BuildTxn(txn *kvstore.Txn) error {
	txn.Writes = []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   rp.request.Key,
			Value: rp.request.Value,
			Cf:    rp.request.Cf,
		}}}
	return nil
}

func (rp *RawPut) Context() *kvrpcpb.Context {
	return rp.request.Context
}

func (rp *RawPut) Response() (interface{}, error) {
	return &kvrpcpb.RawPutResponse{}, nil
}

func (rp *RawPut) RegionError(err *errorpb.Error) interface{} {
	if err == nil {
		return nil
	}

	resp := kvrpcpb.RawPutResponse{}
	resp.RegionError = err
	return &resp
}

// RawDelete implements the Command interface for raw delete requests.
type RawDelete struct {
	request *kvrpcpb.RawDeleteRequest
}

func NewRawDelete(request *kvrpcpb.RawDeleteRequest) RawDelete {
	return RawDelete{request}
}

func (rd *RawDelete) BuildTxn(txn *kvstore.Txn) error {
	txn.Writes = []inner_server.Modify{{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: rd.request.Key,
			Cf:  rd.request.Cf,
		}}}

	return nil
}

func (rd *RawDelete) Context() *kvrpcpb.Context {
	return rd.request.Context
}

func (rd *RawDelete) Response() (interface{}, error) {
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (rd *RawDelete) RegionError(err *errorpb.Error) interface{} {
	if err == nil {
		return nil
	}

	resp := kvrpcpb.RawDeleteResponse{}
	resp.RegionError = err
	return &resp
}

// RawScan implements the Command interface for raw scan requests.
type RawScan struct {
	request  *kvrpcpb.RawScanRequest
	response kvrpcpb.RawScanResponse
}

func NewRawScan(request *kvrpcpb.RawScanRequest) RawScan {
	return RawScan{request, kvrpcpb.RawScanResponse{}}
}

func (rs *RawScan) BuildTxn(txn *kvstore.Txn) error {
	pairs := make([]*kvrpcpb.KvPair, 0)

	it := txn.Reader.IterCF(rs.request.Cf)
	for it.Seek(rs.request.StartKey); it.Valid() && len(pairs) < int(rs.request.Limit); it.Next() {
		key := it.Item().KeyCopy(nil)
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}

		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	rs.response.Kvs = pairs

	return nil
}

func (rs *RawScan) Context() *kvrpcpb.Context {
	return rs.request.Context
}

func (rs *RawScan) Response() (interface{}, error) {
	return &rs.response, nil
}

func (rs *RawScan) RegionError(err *errorpb.Error) interface{} {
	if err == nil {
		return nil
	}

	rs.response.RegionError = err
	return &rs.response
}

type context interface {
	GetContext() *kvrpcpb.Context
}
