package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

type CheckTxnStatus struct {
	CommandBase
	request  *kvrpcpb.CheckTxnStatusRequest
	response *kvrpcpb.CheckTxnStatusResponse
}

func NewCheckTxnStatus(request *kvrpcpb.CheckTxnStatusRequest) CheckTxnStatus {
	response := new(kvrpcpb.CheckTxnStatusResponse)
	return CheckTxnStatus{
		CommandBase: CommandBase{
			context:  request.Context,
			response: response,
		},
		request:  request,
		response: response,
	}
}

func (c *CheckTxnStatus) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.StartTS = &c.request.LockTs
	key := c.request.PrimaryKey
	lock, err := txn.GetLock(key)
	if err != nil {
		return err
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
			c.response.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// Lock has not expired, leave it alone.
			c.response.Action = kvrpcpb.Action_NoAction
			c.response.LockTtl = lock.Ttl
		}

		return nil
	}

	existingWrite, commitTs, err := txn.FindWrite(key, *txn.StartTS)
	if err != nil {
		return err
	}
	if existingWrite == nil {
		// The lock never existed, roll it back.
		write := kvstore.Write{StartTS: *txn.StartTS, Kind: kvstore.WriteKindRollback}
		txn.PutWrite(key, &write, *txn.StartTS)
		c.response.Action = kvrpcpb.Action_LockNotExistRollback

		return nil
	}

	if existingWrite.Kind == kvstore.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		c.response.Action = kvrpcpb.Action_NoAction
		return nil
	}

	// The key has already been committed.
	c.response.CommitVersion = commitTs
	c.response.Action = kvrpcpb.Action_NoAction
	return nil
}

func (c *CheckTxnStatus) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		c.response.RegionError = regionErr
		return c.response
	}

	if e, ok := err.(KeyError); ok {
		c.response.Error = e.KeyErrors()[0]
		return c.response
	}

	return nil
}

func physical(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}

func (c *CheckTxnStatus) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{c.request.PrimaryKey}, nil
}
