package mvcc

import (
	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// DBWriter is the interface to persistent data.
type DBWriter interface {
	Open()
	Close()
	Write(batch WriteBatch) error
	DeleteRange(start, end []byte, latchHandle LatchHandle) error
	NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) WriteBatch
}

type LatchHandle interface {
	AcquireLatches(hashVals []uint64)
	ReleaseLatches(hashVals []uint64)
}

type WriteBatch interface {
	Prewrite(key []byte, lock *MvccLock)
	Commit(key []byte, lock *MvccLock)
	Rollback(key []byte, deleleLock bool)
}

type DBBundle struct {
	DB            *badger.DB
	LockStore     *lockstore.MemStore
	RollbackStore *lockstore.MemStore
}
