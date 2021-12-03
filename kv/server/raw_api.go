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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return &kvrpcpb.RawPutResponse{}, server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{req.Key, req.Value, req.Cf},
	}})
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{Key: req.Key, Cf: req.Cf},
	}})
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	kvs = nil
	acc := 0
	for iter.Seek(req.StartKey); iter.Valid() && acc < int(req.Limit); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		kv := kvrpcpb.KvPair{Key: key, Value: val}
		if len(val) > 0 && err == nil {
			kvs = append(kvs, &kv)
		}
		iter.Close()
		acc += 1
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
