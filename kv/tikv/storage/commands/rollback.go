package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Rollback struct {
	request *kvrpcpb.BatchRollbackRequest
}

func NewRollback(request *kvrpcpb.BatchRollbackRequest) Rollback {
	return Rollback{request}
}

func (r *Rollback) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.StartTS = &r.request.StartVersion
	for _, k := range r.request.Keys {
		rollbackKey(k, txn)
	}
	return nil
}

func rollbackKey(key []byte, txn *kvstore.MvccTxn) error {
	lock, err := txn.GetLock(key)
	if err != nil {
		return err
	}
	if lock == nil || lock.TS != *txn.StartTS {
		return &LockNotFound{key}
	}

	if lock.Kind == kvstore.WriteKindPut {
		txn.DeleteValue(key)
	}

	write := kvstore.Write{StartTS: *txn.StartTS, Kind: kvstore.WriteKindRollback}
	txn.PutWrite(key, &write, *txn.StartTS)
	txn.DeleteLock(key)

	return nil
}

func (r *Rollback) Context() *kvrpcpb.Context {
	return r.request.Context
}

func (r *Rollback) Response() interface{} {
	return &kvrpcpb.BatchRollbackResponse{}
}

func (r *Rollback) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		resp := kvrpcpb.BatchRollbackResponse{}
		resp.RegionError = regionErr
		return &resp
	}

	if e, ok := err.(KeyError); ok {
		resp := kvrpcpb.BatchRollbackResponse{}
		resp.Error = e.keyErrors()[0]
		return &resp
	}

	return nil
}
