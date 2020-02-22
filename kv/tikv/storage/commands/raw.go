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
	request *kvrpcpb.RawGetRequest
}

func NewRawGet(request *kvrpcpb.RawGetRequest) RawGet {
	return RawGet{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (rg *RawGet) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.RawGetResponse)
	val, err := txn.Reader.GetCF(rg.request.Cf, rg.request.Key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			response.NotFound = true
		} else {
			return regionError(err, response)
		}
	} else {
		response.Value = val
	}

	return response, nil
}

// RawPut implements the Command interface for raw put requests.
type RawPut struct {
	CommandBase
	request *kvrpcpb.RawPutRequest
}

func NewRawPut(request *kvrpcpb.RawPutRequest) RawPut {
	return RawPut{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (rp *RawPut) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	txn.Writes = []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   rp.request.Key,
			Value: rp.request.Value,
			Cf:    rp.request.Cf,
		}}}
	return new(kvrpcpb.RawPutResponse), nil
}

func (rp *RawPut) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{rp.request.Key}, nil
}

// RawDelete implements the Command interface for raw delete requests.
type RawDelete struct {
	CommandBase
	request *kvrpcpb.RawDeleteRequest
}

func NewRawDelete(request *kvrpcpb.RawDeleteRequest) RawDelete {
	return RawDelete{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (rd *RawDelete) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	txn.Writes = []inner_server.Modify{{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: rd.request.Key,
			Cf:  rd.request.Cf,
		}}}

	return new(kvrpcpb.RawDeleteResponse), nil
}

func (rd *RawDelete) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{rd.request.Key}, nil
}

// RawScan implements the Command interface for raw scan requests.
type RawScan struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.RawScanRequest
}

func NewRawScan(request *kvrpcpb.RawScanRequest) RawScan {
	return RawScan{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (rs *RawScan) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.RawScanResponse)

	it := txn.Reader.IterCF(rs.request.Cf)
	defer it.Close()
	for it.Seek(rs.request.StartKey); it.Valid() && len(response.Kvs) < int(rs.request.Limit); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return regionError(err, response)
		}

		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}

	return response, nil
}
