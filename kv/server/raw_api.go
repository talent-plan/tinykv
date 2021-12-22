package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse,error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader,err := server.storage.Reader(nil)
	if err != nil{
		log.Fatal(err)
		return nil, err
	}
	defer reader.Close()
	value,err := reader.GetCF(req.GetCf(),req.GetKey())
	if err != nil{
		log.Fatal(err)
		return resp, err
	}
	if value == nil{
		resp.NotFound = true
	}
	resp.Value = value
	return resp,nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(nil,[]storage.Modify{
			{
				Data: storage.Put{
					Key: req.GetKey(),
					Value: req.GetValue(),
					Cf: req.GetCf(),
				},
			},
	})
	if err !=nil{
		log.Fatal(err)
		resp.Error = err.Error()
	}
	return resp,err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(nil,[]storage.Modify{
		{
			Data: storage.Put{
				Key: req.GetKey(),
				Cf: req.GetCf(),
			},
		},
	})
	if err !=nil{
		log.Fatal(err)
		resp.Error = err.Error()
	}
	return resp,err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	reader ,err := server.storage.Reader(nil)
	if err != nil{
		resp.Error = err.Error()
		log.Fatal(err)
		return resp, err
	}
	defer reader.Close()

	it := reader.IterCF(req.GetCf())
	defer it.Close()
	limitTimes := req.GetLimit()
	loopTimes := uint32(0)
	startKey := req.GetStartKey()
	for it.Seek(startKey) ; loopTimes<limitTimes && it.Valid() ; it.Next(){
		item := it.Item()
		kvs := kvrpcpb.KvPair{}
		k  := item.Key()
		v ,err:= item.Value()
		kvs.Key = k
		kvs.Value = v
		if err!= nil{
			//TODO
		}
		resp.Kvs = append(resp.Kvs,&kvs)
		loopTimes ++
	}
	return resp, err
}
