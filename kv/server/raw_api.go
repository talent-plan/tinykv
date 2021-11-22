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
	cf := req.Cf
	key := req.Key
	value, err := reader.GetCF(cf, key)
	if err != nil {
		return nil, err
	}
	resp := kvrpcpb.RawGetResponse{}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = append(resp.Value, value...)
	}
	reader.Close()
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	cf := req.Cf
	key := req.Key
	value := req.Value
	modify := []storage.Modify{
		{
			Data: storage.Put{
				Key:   key,
				Value: value,
				Cf:    cf,
			},
		},
	}
	if err := server.storage.Write(nil, modify); err != nil {
		return nil, err
	}
	resp := kvrpcpb.RawPutResponse{}
	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	cf := req.Cf
	key := req.Key
	modify := []storage.Modify{
		{
			Data: storage.Delete{
				Key: key,
				Cf:  cf,
			},
		},
	}
	if err := server.storage.Write(nil, modify); err != nil {
		return nil, err
	}
	resp := kvrpcpb.RawDeleteResponse{}
	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	cf, startKey, limit := req.Cf, req.StartKey, req.Limit

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	it := reader.IterCF(cf)
	it.Seek(startKey)

	kvs := []*kvrpcpb.KvPair{}
	for ; it.Valid() && len(kvs) < int(limit); it.Next() {
		key := it.Item().KeyCopy(nil)

		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: value})

	}
	it.Close()
	resp := kvrpcpb.RawScanResponse{Kvs: kvs}
	return &resp, nil
}
