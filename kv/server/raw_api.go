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
	ret := &kvrpcpb.RawGetResponse{}

	reader, _ := server.storage.Reader(nil)
	val, err := reader.GetCF(
		req.GetCf(),
		req.GetKey(),
	)

	if err != nil {
		ret.NotFound = false
		return ret,err
	}

	ret.Value = val
	ret.NotFound = true
	return ret, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	ret := &kvrpcpb.RawPutResponse{}

	err := server.storage.Write(nil,
		[]storage.Modify{
			{
				Data: storage.Put{
					Key: req.GetKey(),
					Cf: req.GetCf(),
					Value: req.GetValue(),
				},
			},
		})
	if err != nil {
		ret.Error = err.Error()
		return ret,err
	}

	return ret, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	ret := &kvrpcpb.RawDeleteResponse{}

	err := server.storage.Write(nil,
		[]storage.Modify{
			{
				Data: storage.Delete{
					Key: req.GetKey(),
					Cf: req.GetCf(),
				},
			},
		})
	if err != nil {
		return ret,err
	}

	return ret, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	ret := &kvrpcpb.RawScanResponse{}
	reader,_ := server.storage.Reader(nil)
	it := reader.IterCF(req.GetCf())
	it.Seek(req.GetStartKey())

	var err error
	var kvs []*kvrpcpb.KvPair
	for i:= 0; uint32(i) < req.GetLimit(); i++ {
		if !it.Valid() {
			break
		}

		k := it.Item().Key()
		var v []byte
		v, err = it.Item().Value()
		if err != nil {
			break
		}

		pair := kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		}
		kvs = append(kvs, &pair)
		it.Next()
	}

	if err != nil {
		ret.Error = err.Error()
		return ret,err
	}

	ret.Kvs = kvs
	return ret, nil
}
