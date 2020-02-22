package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// This file contains some base types for commands to reduce boilerplate.

type CommandBase struct {
	response interface{}
	context  *kvrpcpb.Context
}

func (base CommandBase) Context() *kvrpcpb.Context {
	return base.context
}

func (base CommandBase) Response() interface{} {
	return base.response
}

type ReadOnly struct{}

func (ro ReadOnly) WillWrite(_ dbreader.DBReader) ([][]byte, error) {
	return [][]byte{}, nil
}
