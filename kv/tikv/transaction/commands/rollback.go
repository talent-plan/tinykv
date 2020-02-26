package commands

import (
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/tikv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Rollback struct {
	CommandBase
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (r *Rollback) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.BatchRollbackResponse)
	txn.StartTS = &r.request.StartVersion

	for _, k := range r.request.Keys {
		resp, err := rollbackKey(k, txn, response)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return response, nil
}

func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return regionError(err, response)
	}

	if lock == nil || lock.Ts != *txn.StartTS {
		// There is no lock, check the write status.
		existingWrite, ts, err := txn.FindWrite(key, *txn.StartTS)
		if err != nil {
			return regionError(err, response)
		}
		if existingWrite == nil {
			// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
			write := mvcc.Write{StartTS: *txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, &write, *txn.StartTS)

			return nil, nil
		} else {
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				return nil, nil
			}

			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}

	write := mvcc.Write{StartTS: *txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, &write, *txn.StartTS)
	txn.DeleteLock(key)

	return nil, nil
}

func (r *Rollback) WillWrite() [][]byte {
	return r.request.Keys
}
