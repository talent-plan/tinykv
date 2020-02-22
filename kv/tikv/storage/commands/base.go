package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
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

type ReadOnly struct{}

func (ro ReadOnly) WillWrite(_ dbreader.DBReader) ([][]byte, error) {
	return [][]byte{}, nil
}

func regionError(err error, resp interface{}) (interface{}, error) {
	if regionErr, ok := err.(*inner_server.RegionError); ok {
		respValue := reflect.ValueOf(resp)
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionErr.RequestErr))
		return resp, nil
	}

	return nil, err
}
