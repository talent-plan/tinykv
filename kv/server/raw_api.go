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
	reader, _ := server.storage.Reader(req.GetContext())
	defer reader.Close()

	resp := &kvrpcpb.RawGetResponse{}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
	}

	if len(value) == 0 {
		resp.NotFound = true
	}
	resp.Value = value

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Put{Cf: req.GetCf(), Key: req.GetKey(), Value: req.GetValue()},
		},
	})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Delete{Cf: req.GetCf(), Key: req.GetKey()},
		},
	})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(req.GetContext())
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	kvs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.GetStartKey()); iter.Valid() && len(kvs) < int(req.GetLimit()); iter.Next() {
		item := iter.Item()
		kv := &kvrpcpb.KvPair{Key: item.Key()}
		if value, err := item.Value(); err == nil {
			kv.Value = value
		}
		kvs = append(kvs, kv)
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
