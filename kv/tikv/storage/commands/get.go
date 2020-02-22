package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (g *Get) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	key := g.request.Key
	txn.StartTS = &g.request.Version
	response := new(kvrpcpb.GetResponse)

	// Check for locks.
	lock, err := txn.GetLock(key)
	if err != nil {
		return regionError(err, response)
	}
	if lock.IsLockedFor(key, *txn.StartTS, response) {
		// Key is locked.
		return response, nil
	}

	// Search writes for a committed value.
	value, err := txn.FindWrittenValue(key, *txn.StartTS)
	if err != nil {
		return regionError(err, response)
	}

	response.Value = value
	return response, nil
}
