package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Get struct {
	request  *kvrpcpb.GetRequest
	response kvrpcpb.GetResponse
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		request,
		kvrpcpb.GetResponse{},
	}
}

func (g *Get) BuildTxn(txn *kvstore.MvccTxn) error {
	key := g.request.Key
	txn.StartTS = &g.request.Version

	// Check for locks.
	lock, err := txn.GetLock(key)
	if err != nil {
		return err
	}
	if lock.IsLockedFor(key, *txn.StartTS) {
		// Key is locked.
		return &kvstore.LockedError{Info: []kvrpcpb.LockInfo{*lock.Info(key)}}
	}

	// Search writes for a committed value.
	value, err := txn.FindWrittenValue(key, *txn.StartTS)
	if err != nil {
		return err
	}

	// If we got this far, then we found a valid value and it was not locked, so lets add it to the response.
	g.response.Value = value
	return nil
}

func (g *Get) Context() *kvrpcpb.Context {
	return g.request.Context
}

func (g *Get) Response() interface{} {
	return &g.response
}

func (g *Get) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		g.response.RegionError = regionErr
		return &g.response
	}

	if e, ok := err.(KeyError); ok {
		keyErrs := e.KeyErrors()
		if len(keyErrs) > 0 {
			g.response.Error = keyErrs[0]
			return &g.response
		}
	}

	return nil
}

func (g *Get) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	return [][]byte{}, nil
}
