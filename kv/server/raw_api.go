package server

import (
	"context"

	"github.com/Connor1996/badger"
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
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: err == badger.ErrKeyNotFound,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
			Value: val,
			NotFound: true,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key: req.GetKey(), 
			Value: req.GetValue(), 
			Cf: req.GetCf()}})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf: req.GetCf(),
		},
	})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	defer reader.Close()

	it := reader.IterCF(req.GetCf())
	defer it.Close()
	n := req.GetLimit()
	var kvs []*kvrpcpb.KvPair
	for it.Seek(req.GetStartKey()); it.Valid(); it.Next() {
		item := it.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key: item.KeyCopy(nil),
			Value: val,
		})
		n--
		if n == 0 {
			break
		}

	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
