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
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	result := kvrpcpb.RawGetResponse{
		Value: value,
	}
	// check not found error
	if value == nil {
		result.NotFound = true
	}

	return &result, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var data []storage.Modify
	data = append(data, storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	})

	err := server.storage.Write(req.Context, data)
	result := &kvrpcpb.RawPutResponse{}
	return result, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var data []storage.Modify
	data = append(data, storage.Modify{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	})

	err := server.storage.Write(req.Context, data)
	result := &kvrpcpb.RawDeleteResponse{}
	return result, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	counter := req.Limit
	var kvs []*kvrpcpb.KvPair
	for iter.Valid() && counter > 0 {
		item := iter.Item()
		value, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		iter.Next()
		counter -= 1
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, err
}
