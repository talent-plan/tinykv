package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var response kvrpcpb.RawGetResponse
	rd, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = err.Error()
		return &response, err
	}
	value, err := rd.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		response.Error = err.Error()
		return &response, err
	}
	response.Value = value
	if value == nil {
		response.NotFound = true
	} else {
		response.NotFound = false
	}
	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var response kvrpcpb.RawPutResponse
	var putBatch []storage.Modify
	putBatch = append(putBatch, storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		}})
	err := server.storage.Write(req.GetContext(), putBatch)
	if err != nil {
		response.Error = err.Error()
	}
	return &response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var response kvrpcpb.RawDeleteResponse
	var delBatch []storage.Modify
	delBatch = append(delBatch, storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		}})
	err := server.storage.Write(req.GetContext(), delBatch)
	if err != nil {
		response.Error = err.Error()
	}
	return &response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var response kvrpcpb.RawScanResponse
	var kvs []*kvrpcpb.KvPair
	rd, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = err.Error()
		return &response, err
	}
	iter := rd.IterCF(req.GetCf())
	//the limit in req
	length := req.GetLimit()
	for iter.Seek(req.GetStartKey()); iter.Valid() && length > 0; iter.Next() {
		if length <= 0 {
			break
		}
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			response.Error = err.Error()
			return &response, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		length--
	}
	response.Kvs = kvs
	return &response, nil
}
