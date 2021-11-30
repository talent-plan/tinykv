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
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := kvrpcpb.RawGetResponse{}
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	} else {
		resp.NotFound = false
	}
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	server.storage.Write(nil, []storage.Modify{{Data: put}})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delReq := storage.Delete{Key: req.Key, Cf: req.Cf}
	err := server.storage.Write(nil, []storage.Modify{{Data: delReq}})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iterator := reader.IterCF(req.Cf)
	if err != nil {
		return nil, err
	}
	iterator.Seek(req.StartKey)
	pair := make([]*kvrpcpb.KvPair, 0)
	var cnt uint32
	cnt = 0
	for cnt < req.Limit && iterator.Valid() {
		item := iterator.Item()
		value, _ := item.Value()
		pair = append(pair, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		cnt++
		iterator.Next()
	}
	resp := kvrpcpb.RawScanResponse{}
	resp.Kvs = pair
	return &resp, nil
}
