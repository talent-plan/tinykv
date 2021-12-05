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
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	notFound := false
	if value == nil {notFound = true}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: notFound}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{storage.Put{req.Key, req.Value, req.Cf}}
	return nil, server.storage.Write(req.Context, []storage.Modify{modify})
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{storage.Delete{req.Key, req.Cf}}
	return nil, server.storage.Write(req.Context, []storage.Modify{modify})
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	var pairs []*kvrpcpb.KvPair
	for limit := req.Limit; limit > 0 && iter.Valid(); limit-- {
		item := iter.Item()
		value, _ := item.Value()
		pairs = append(pairs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: pairs}, nil
}
