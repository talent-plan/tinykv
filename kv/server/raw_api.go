package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	reader , _ := server.storage.Reader(req.Context)
	value , err := reader.GetCF(cf, key)
	response := &kvrpcpb.RawGetResponse{
		Value: value,
		NotFound: false,
	}
	if value == nil {
		response.Value = nil
		response.NotFound = true
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	cf := req.GetCf()
	key := req.GetKey()
	value := req.GetValue()
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:  cf,
				Key: key,
				Value: value,
			},
		},
	})
	putResponse := &kvrpcpb.RawPutResponse{}
	if err != nil {
		putResponse.Error = err.Error()
	}
	return putResponse, err
 
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.GetKey()
	cf := req.GetCf()

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf: cf,
				Key: key,
			},
		},
	})

	delResponse := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		delResponse.Error = err.Error()
	}
	return delResponse, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	reader , _ := server.storage.Reader(nil)
	iterator := reader.IterCF(cf)

	var kvs []*kvrpcpb.KvPair
	var err error
	iterator.Seek(startKey)
	for i := 0; uint32(i) < limit; i++ {
		if !iterator.Valid() {
			break
		}
		item := iterator.Item()
		key := item.Key()
		value , err := item.Value()
		if err != nil {
			fmt.Printf("ERROR: Failed to get Value from key: %v\n", key)
			break;
		}
		pair := kvrpcpb.KvPair{
			Key: key,
			Value: value,
		}
		kvs = append(kvs, &pair)
		iterator.Next()
	}
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err
}
