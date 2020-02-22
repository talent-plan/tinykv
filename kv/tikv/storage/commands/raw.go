package commands

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// RawGet implements the Command interface for raw get requests.
type RawGet struct {
	ReadOnly
	CommandBase
	request  *kvrpcpb.RawGetRequest
	response *kvrpcpb.RawGetResponse
}

func NewRawGet(request *kvrpcpb.RawGetRequest) RawGet {
	response := new(kvrpcpb.RawGetResponse)
	return RawGet{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
}

func (rg *RawGet) BuildTxn(txn *kvstore.MvccTxn) error {
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

func (rg *RawGet) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		rg.response.RegionError = regionErr
		return rg.response
	}

	return nil
}

// RawPut implements the Command interface for raw put requests.
type RawPut struct {
	CommandBase
	request  *kvrpcpb.RawPutRequest
	response *kvrpcpb.RawPutResponse
}

func NewRawPut(request *kvrpcpb.RawPutRequest) RawPut {
	response := new(kvrpcpb.RawPutResponse)
	return RawPut{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
}

func (rp *RawPut) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.Writes = []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   rp.request.Key,
			Value: rp.request.Value,
			Cf:    rp.request.Cf,
		}}}
	return nil
}

func (rp *RawPut) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		rp.response.RegionError = regionErr
		return rp.response
	}

	return nil
}

func (rp *RawPut) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{rp.request.Key}, nil
}

// RawDelete implements the Command interface for raw delete requests.
type RawDelete struct {
	CommandBase
	request  *kvrpcpb.RawDeleteRequest
	response *kvrpcpb.RawDeleteResponse
}

func NewRawDelete(request *kvrpcpb.RawDeleteRequest) RawDelete {
	response := new(kvrpcpb.RawDeleteResponse)
	return RawDelete{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
}

func (rd *RawDelete) BuildTxn(txn *kvstore.MvccTxn) error {
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
		rd.response.RegionError = regionErr
		return rd.response
	}

	return nil
}

func (rd *RawDelete) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{rd.request.Key}, nil
}

// RawScan implements the Command interface for raw scan requests.
type RawScan struct {
	ReadOnly
	CommandBase
	request  *kvrpcpb.RawScanRequest
	response *kvrpcpb.RawScanResponse
}

func NewRawScan(request *kvrpcpb.RawScanRequest) RawScan {
	response := new(kvrpcpb.RawScanResponse)
	return RawScan{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
}

func (rs *RawScan) BuildTxn(txn *kvstore.MvccTxn) error {
	pairs := make([]*kvrpcpb.KvPair, 0)

	it := txn.Reader.IterCF(rs.request.Cf)
	defer it.Close()
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

func (rs *RawScan) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		rs.response.RegionError = regionErr
		return rs.response
	}

	return nil
}
