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
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val != nil {
		resp.Value = val
		resp.NotFound = false
	} else {
		resp.Error = err.Error()
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := make([]storage.Modify, 1)
	batch[0].Data = storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := make([]storage.Modify, 1)
	batch[0].Data = storage.Delete{Key: req.Key, Cf: req.Cf}
	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	kvNum := 0
	for ; iter.Valid(); iter.Next() {

		val, _ := iter.Item().Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Error: nil, Key: iter.Item().Key(), Value: val})
		kvNum += 1

		if kvNum >= int(req.Limit) {
			break
		}

	}

	return resp, nil
}
