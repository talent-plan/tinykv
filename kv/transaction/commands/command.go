package commands

// TODO delete the commands package.

import (
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Command is an abstraction which covers the process from receiving a request from gRPC to returning a response.
type Command interface {
	Context() *kvrpcpb.Context
	StartTs() uint64
	// WillWrite returns a list of all keys that might be written by this command. Return nil if the command is readonly.
	WillWrite() [][]byte
	// Read executes a readonly part of the command. Only called if WillWrite returns nil. If the command needs to write
	// to the DB it should return a non-nil set of keys that the command will write.
	Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error)
	// PrepareWrites is for building writes in an mvcc transaction. Commands can also make non-transactional
	// reads and writes using txn. Returning without modifying txn means that no transaction will be executed.
	PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error)
}

// Run runs a transactional command.
func RunCommand(cmd Command, storage storage.Storage, latches *latches.Latches) (interface{}, error) {
	ctxt := cmd.Context()
	var resp interface{}

	keysToWrite := cmd.WillWrite()
	if keysToWrite == nil {
		// The command is readonly or requires access to the DB to determine the keys it will write.
		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		txn := mvcc.RoTxn{Reader: reader, StartTS: cmd.StartTs()}
		resp, keysToWrite, err = cmd.Read(&txn)
		reader.Close()
		if err != nil {
			return nil, err
		}
	}

	if keysToWrite != nil {
		// The command will write to the DB.

		latches.WaitForLatches(keysToWrite)
		defer latches.ReleaseLatches(keysToWrite)

		reader, err := storage.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		// Build an mvcc transaction.
		txn := mvcc.NewTxn(reader, cmd.StartTs())
		resp, err = cmd.PrepareWrites(&txn)
		if err != nil {
			return nil, err
		}

		latches.Validate(&txn, keysToWrite)

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		err = storage.Write(ctxt, txn.Writes())
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// CommandBase provides some default function implementations for the Command interface.
type CommandBase struct {
	context *kvrpcpb.Context
	startTs uint64
}

func (base CommandBase) Context() *kvrpcpb.Context {
	return base.context
}

func (base CommandBase) StartTs() uint64 {
	return base.startTs
}

func (base CommandBase) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	return nil, nil, nil
}

// ReadOnly is a helper type for commands which will never write anything to the database. It provides some default
// function implementations.
type ReadOnly struct{}

func (ro ReadOnly) WillWrite() [][]byte {
	return nil
}

func (ro ReadOnly) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	return nil, nil
}
