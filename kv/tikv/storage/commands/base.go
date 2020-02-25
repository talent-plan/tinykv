package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"reflect"
)

// This file contains some base types for commands to reduce boilerplate.

type CommandBase struct {
	context *kvrpcpb.Context
}

func (base CommandBase) Context() *kvrpcpb.Context {
	return base.context
}

func (base CommandBase) Read(txn *kvstore.RoTxn) (interface{}, [][]byte, error) {
	return nil, nil, nil
}

type ReadOnly struct{}

func (ro ReadOnly) WillWrite() [][]byte {
	return nil
}

func (ro ReadOnly) PrepareWrites(txn *kvstore.MvccTxn) (interface{}, error) {
	return nil, nil
}

func regionError(err error, resp interface{}) (interface{}, error) {
	if regionErr, ok := err.(*inner_server.RegionError); ok {
		respValue := reflect.ValueOf(resp)
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionErr.RequestErr))
		return resp, nil
	}

	return nil, err
}

// regionErrorRo is a convenience version of regionError to match the return type of Read.
func regionErrorRo(err error, resp interface{}) (interface{}, [][]byte, error) {
	resp, err = regionError(err, resp)
	return resp, nil, err
}
