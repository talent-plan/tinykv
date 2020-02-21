package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type ResolveLock struct {
	request  *kvrpcpb.ResolveLockRequest
	keyLocks []kvstore.KlPair
}

func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{request: request}
}

func (rl *ResolveLock) BuildTxn(txn *kvstore.MvccTxn) error {
	// A map from start timestamps to commit timestamps which tells us whether a transaction (identified by start ts)
	// has been committed (and if so, then its commit ts) or rolled back (in which case the commit ts is 0).
	txn.StartTS = &rl.request.StartVersion
	commitTs := rl.request.CommitVersion

	for _, kl := range rl.keyLocks {
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

func (rl *ResolveLock) Context() *kvrpcpb.Context {
	return rl.request.Context
}

func (rl *ResolveLock) Response() interface{} {
	return &kvrpcpb.ResolveLockResponse{}
}

func (rl *ResolveLock) HandleError(err error) interface{} {
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

func (rl *ResolveLock) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	// Find all locks where the lock's transaction (start ts) is in txnStatus.
	keyLocks, err := kvstore.AllLocksForTxn(rl.request.StartVersion, reader)
	if err != nil {
		return nil, err
	}
	rl.keyLocks = keyLocks
	var keys [][]byte
	for _, kl := range keyLocks {
		keys = append(keys, kl.Key)
	}
	return keys, nil
}
