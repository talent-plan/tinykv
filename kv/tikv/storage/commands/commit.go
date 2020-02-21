package commands

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"

	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Commit struct {
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{request}
}

func (c *Commit) BuildTxn(txn *kvstore.MvccTxn) error {
	commitTs := c.request.CommitVersion
	startTs := c.request.StartVersion
	if commitTs <= startTs {
		return fmt.Errorf("invalid transaction timestamp: %d (commit TS) <= %d (start TS)", commitTs, startTs)
	}

	// Commit each key.
	txn.StartTS = &startTs
	for _, k := range c.request.Keys {
		e := commitKey(k, commitTs, txn)
		if e != nil {
			return e
		}
	}

	return nil
}

func commitKey(key []byte, commitTs uint64, txn *kvstore.MvccTxn) error {
	lock, err := txn.GetLock(key)
	if err != nil {
		return err
	}
	if lock == nil {
		return nil
	}

	if lock.Ts != *txn.StartTS {
		// Key is locked by a different transaction.
		write, _, err := txn.FindWrite(key, *txn.StartTS)
		if err != nil {
			return err
		}
		if write == nil || write.Kind == kvstore.WriteKindRollback {
			// Transaction has been rolled back.
			return &LockNotFound{key}
		} else {
			// Already committed.
			return nil
		}
	}

	// Commit a Write object to the DB
	write := kvstore.Write{StartTS: *txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, &write, commitTs)
	// Unlock the key
	txn.DeleteLock(key)

	return nil
}

func (c *Commit) Context() *kvrpcpb.Context {
	return c.request.Context
}

func (c *Commit) Response() interface{} {
	return &kvrpcpb.CommitResponse{}
}

func (c *Commit) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		resp := kvrpcpb.CommitResponse{}
		resp.RegionError = regionErr
		return &resp
	}

	if e, ok := err.(KeyError); ok {
		resp := kvrpcpb.CommitResponse{}
		resp.Error = e.KeyErrors()[0]
		return &resp
	}

	return nil
}

func (c *Commit) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return c.request.Keys, nil
}
