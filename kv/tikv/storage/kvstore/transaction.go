package kvstore

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
)

// Txn represents an internal transaction (see tikv/storage/doc.go for a definition). It permits reading from a snapshot
// and stores writes in a buffer for atomic writing.
type Txn struct {
	Reader dbreader.DBReader
	Writes []inner_server.Modify
}

func NewTxn(reader dbreader.DBReader) Txn {
	return Txn{
		reader,
		nil,
	}
}
