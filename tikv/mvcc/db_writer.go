package mvcc

import (
	"sync"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pkg/kvrpcpb"
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
	Prewrite(key []byte, lock *MvccLock, isPessimisticLock bool)
	Commit(key []byte, lock *MvccLock)
	Rollback(key []byte, deleleLock bool)
	PessimisticLock(key []byte, lock *MvccLock)
	PessimisticRollback(key []byte)
}

type DBBundle struct {
	DB            *badger.DB
	LockStore     *lockstore.MemStore
	RollbackStore *lockstore.MemStore
	MemStoreMu    sync.Mutex
}

type DBSnapshot struct {
	Txn           *badger.Txn
	LockStore     *lockstore.MemStore
	RollbackStore *lockstore.MemStore
}

func NewDBSnapshot(db *DBBundle) *DBSnapshot {
	return &DBSnapshot{
		Txn:           db.DB.NewTransaction(false),
		LockStore:     db.LockStore,
		RollbackStore: db.RollbackStore,
	}
}
