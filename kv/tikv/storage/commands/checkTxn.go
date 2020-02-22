package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

type CheckTxnStatus struct {
	CommandBase
	request *kvrpcpb.CheckTxnStatusRequest
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (c *CheckTxnStatus) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	txn.StartTS = &c.request.LockTs
	key := c.request.PrimaryKey
	response := new(kvrpcpb.CheckTxnStatusResponse)

	lock, err := txn.GetLock(key)
	if err != nil {
		return regionError(err, response)
	}
	if lock != nil && lock.Ts == *txn.StartTS {
		if physical(lock.Ts)+lock.Ttl < physical(c.request.CurrentTs) {
			// Lock has expired, roll it back.
			write := kvstore.Write{StartTS: *txn.StartTS, Kind: kvstore.WriteKindRollback}
			if lock.Kind == kvstore.WriteKindPut {
				txn.DeleteValue(key)
			}
			txn.PutWrite(key, &write, *txn.StartTS)
			txn.DeleteLock(key)
			response.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// Lock has not expired, leave it alone.
			response.Action = kvrpcpb.Action_NoAction
			response.LockTtl = lock.Ttl
		}

		return response, nil
	}

	existingWrite, commitTs, err := txn.FindWrite(key, *txn.StartTS)
	if err != nil {
		return regionError(err, response)
	}
	if existingWrite == nil {
		// The lock never existed, roll it back.
		write := kvstore.Write{StartTS: *txn.StartTS, Kind: kvstore.WriteKindRollback}
		txn.PutWrite(key, &write, *txn.StartTS)
		response.Action = kvrpcpb.Action_LockNotExistRollback

		return response, nil
	}

	if existingWrite.Kind == kvstore.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		response.Action = kvrpcpb.Action_NoAction
		return response, nil
	}

	// The key has already been committed.
	response.CommitVersion = commitTs
	response.Action = kvrpcpb.Action_NoAction
	return response, nil
}

func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{c.request.PrimaryKey}, nil
}
