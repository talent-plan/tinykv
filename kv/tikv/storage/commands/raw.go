package commands

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
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

func (rg *RawGet) Response() interface{} {
	return &rg.response
}

func (rg *RawGet) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		rg.response.RegionError = regionErr
		return &rg.response
	}

	return nil
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

func (rp *RawPut) Response() interface{} {
	return &kvrpcpb.RawPutResponse{}
}

func (rp *RawPut) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		resp := kvrpcpb.RawPutResponse{}
		resp.RegionError = regionErr
		return &resp
	}

	return nil
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

func (rd *RawDelete) Response() interface{} {
	return &kvrpcpb.RawDeleteResponse{}
}

func (rd *RawDelete) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		resp := kvrpcpb.RawDeleteResponse{}
		resp.RegionError = regionErr
		return &resp
	}

	return nil
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
		item := it.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
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

func (rs *RawScan) Response() interface{} {
	return &rs.response
}

func (rs *RawScan) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		rs.response.RegionError = regionErr
		return &rs.response
	}

	return nil
}
