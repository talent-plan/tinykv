package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
			startTs: request.Version,
		},
		request: request,
	}
}

func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	response := new(kvrpcpb.GetResponse)

	// Check for locks.
	lock, err := txn.GetLock(key)
	if err != nil {
		return regionErrorRo(err, response)
	}
	if lock.IsLockedFor(key, txn.StartTS, response) {
		// Key is locked.
		return response, nil, nil
	}

	// Search writes for a committed value.
	value, err := txn.GetValue(key)
	if err != nil {
		return regionErrorRo(err, response)
	}

	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}

	return response, nil, nil
}
