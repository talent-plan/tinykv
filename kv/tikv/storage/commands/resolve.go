package commands

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type ResolveLock struct {
	request *kvrpcpb.ResolveLockRequest
}

func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{request: request}
}

func (c *ResolveLock) BuildTxn(txn *kvstore.MvccTxn) error {
	if len(c.request.Keys) != 0 {
		// In TiKV, this is a ResolveLockLite command.

		if c.request.StartVersion == 0 {
			return errors.New("storage: malformed resolve lock request: Keys is not empty and StartVersion is 0")
		}

		txn.StartTS = &c.request.StartVersion
		commitTs := c.request.CommitVersion
		if commitTs == 0 {
			// Rollback all keys.
			for _, key := range c.request.Keys {
				err := rollbackKey(key, txn)
				if err != nil {
					return err
				}
			}
		} else {
			// Commit all keys.
			for _, key := range c.request.Keys {
				err := commitKey(key, commitTs, txn)
				if err != nil {
					// Ignore lock not found errors.
					_, ok := err.(*LockNotFound)
					if !ok {
						return err
					}
				}
			}
		}

		return nil
	}

	// In TiKV, this is a batched ResolveLock command.

	// A map from start timestamps to commit timestamps which tells us whether a transaction (identified by start ts)
	// has been committed (and if so, then its commit ts) or rolled back (in which case the commit ts is 0).
	txnStatus := map[uint64]uint64{}
	txns := make([]uint64, len(c.request.TxnInfos))
	if c.request.StartVersion == 0 {
		// Resolve multiple transactions.
		for i, info := range c.request.TxnInfos {
			txnStatus[info.Txn] = info.Status
			txns[i] = info.Txn
		}
	} else {
		// Resolve a single transaction.
		txnStatus[c.request.StartVersion] = c.request.CommitVersion
	}

	// Find all locks where the lock's transaction (start ts) is in txnStatus.
	keyLocks, err := txn.AllLocksForTxns(txnSet{txnStatus})
	if err != nil {
		return err
	}
	for _, kl := range keyLocks {
		txn.StartTS = &kl.Lock.Ts
		commitTs := txnStatus[kl.Lock.Ts]
		if commitTs == 0 {
			err := rollbackKey(kl.Key, txn)
			if err != nil {
				return err
			}
		} else {
			err := commitKey(kl.Key, commitTs, txn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Helper struct to implement the kvstore.TxnSet interface for a map from uint64 -> uint64.
type txnSet struct {
	inner map[uint64]uint64
}

func (set txnSet) Contains(ts uint64) bool {
	_, ok := set.inner[ts]
	return ok
}

func (c *ResolveLock) Context() *kvrpcpb.Context {
	return c.request.Context
}

func (c *ResolveLock) Response() interface{} {
	return &kvrpcpb.ResolveLockResponse{}
}

func (c *ResolveLock) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		var resp kvrpcpb.ResolveLockResponse
		resp.RegionError = regionErr
		return &resp
	}

	if e, ok := err.(KeyError); ok {
		var resp kvrpcpb.ResolveLockResponse
		resp.Error = e.KeyErrors()[0]
		return &resp
	}

	return nil
}
