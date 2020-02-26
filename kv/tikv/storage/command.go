package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"reflect"
)

// Command is an abstraction which covers the process from receiving a request from gRPC to returning a response.
// That process is driven by a Scheduler.
type Command interface {
	Context() *kvrpcpb.Context
	// WillWrite returns a list of all keys that might be written by this command. Return nil if the command is readonly.
	WillWrite() [][]byte
	// Read executes a readonly part of the command. Only called if WillWrite returns nil. If the command needs to write
	// to the DB it should return a non-nil set of keys that the command will write.
	Read(txn *kvstore.RoTxn) (interface{}, [][]byte, error)
	// PrepareWrites is for building writes in an mvcc transaction. Commands can also make non-transactional
	// reads and writes using txn. Returning without modifying txn means that no transaction will be executed.
	PrepareWrites(txn *kvstore.MvccTxn) (interface{}, error)
}

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
