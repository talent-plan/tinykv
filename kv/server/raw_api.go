package server

import (
	"context"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory
// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	switch err {
	case nil:
		return &kvrpcpb.RawGetResponse{
			Value: value,
		}, nil
	case badger.ErrKeyNotFound:
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	default:
		return nil, err
	}
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modify := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}
	kvCtx := &kvrpcpb.Context{}
	err := server.storage.Write(kvCtx, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{Error: ""}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	err := server.storage.Write(&kvrpcpb.Context{}, []storage.Modify{modify})
	switch err {
	case nil:
	case badger.ErrKeyNotFound:
	default:
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	var ret []*kvrpcpb.KvPair
	// iter.Seek(req.StartKey)
	iter := reader.IterCF(req.Cf)
	count := 0
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if uint32(count) == req.Limit {
			break
		}
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		ret = append(ret, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
		count++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: ret,
	}, nil
}
