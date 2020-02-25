package interfaces

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Interfaces used with the scheduler and related code in storage.

// Scheduler takes Commands and runs them asynchronously. It is up to implementations to decide the scheduling policy.
type Scheduler interface {
	// Run executes a command asynchronously and returns a channel over which the result of the command execution is
	// sent.
	Run(Command) <-chan SchedResult
	Stop()
}

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

// SchedResult is a 'generic' result type for responses. It is used to return a Response/error pair over channels where
// we can't use Go's multiple return values.
type SchedResult struct {
	Response interface{}
	Err      error
}

func RespOk(resp interface{}) SchedResult {
	return SchedResult{
		Response: resp,
		Err:      nil,
	}
}

func RespErr(err error) SchedResult {
	return SchedResult{
		Response: nil,
		Err:      err,
	}
}
