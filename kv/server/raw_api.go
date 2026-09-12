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
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	if req.Limit == 0 {
		return &kvrpcpb.RawScanResponse{
			Kvs: []*kvrpcpb.KvPair{},
		}, nil
	}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	var kvs []*kvrpcpb.KvPair
	for iter.Seek(req.StartKey); iter.Valid() && len(kvs) < int(req.Limit); iter.Next() {
		item := iter.Item()

		key := item.KeyCopy(nil)

		val, err := item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, err
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			// эти данные действительны только до перемещения итератора, после этого бд может делать с ними что хочет.
			// поэтому нужно копировать их
			// тут стоит задуматься о концепции владения
			Key:   key,
			Value: val,
		})
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
