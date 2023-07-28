package server

import (
	"context"
	"log"

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
	reader, _ := server.storage.Reader(req.Context)
	value, err := reader.GetCF(cf, key)
	// 封装grpc response
	response := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}
	if value == nil {
		response.Value = nil
		response.NotFound = true
	}
	if err != nil {
		log.Fatal("RawGet error:", err)
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
	// Write时需要追一下Modify内部实现，本质上用的是一个泛型。将Modify内部Data要绑定到一种操作类型的结构体上，结构体内部包含操作的数据
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    cf,
				Key:   key,
				Value: value,
			},
		},
	})
	putResponse := &kvrpcpb.RawPutResponse{}
	if err != nil {
		log.Fatal("RawPut error:", err)
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
				Cf:  cf,
				Key: key,
			},
		},
	})

	delResponse := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		log.Fatal("RawDelete error:", err)
		delResponse.Error = err.Error()
	}
	return delResponse, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
// RawScan稍微复杂一些，需要看一下RawScanRequest传过来什么
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	reader, _ := server.storage.Reader(nil)
	// 迭代器BadgerIterator的Valid函数是判断当前位置是否有效，而不是判断Next是否有效
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
		value, err := item.Value()
		if err != nil {
			log.Fatal("ERROR: Failed to get Value from key:", key)
			break
		}
		pair := kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		kvs = append(kvs, &pair)
		iterator.Next()
	}
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
		// err为空时不能调用err.Error(),特判一下
		//Error : err.Error(),
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err
}
