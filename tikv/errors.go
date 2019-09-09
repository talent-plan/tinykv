package tikv

import (
	"fmt"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key     []byte
	Primary []byte
	StartTS uint64
	TTL     uint64
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, primary: %q, startTS: %v", e.Key, e.Primary, e.StartTS)
}

// ErrRetryable suggests that client may restart the txn. e.g. write conflict.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

var (
	ErrLockNotFound    = ErrRetryable("lock not found")
	ErrAlreadyRollback = ErrRetryable("already rollback")
	ErrReplaced        = ErrRetryable("replaced by another transaction")
)

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

type ErrKeyAlreadyExists struct {
	Key []byte
}

func (e ErrKeyAlreadyExists) Error() string {
	return "key already exists"
}

type ErrDeadlock struct {
	LockKey         []byte
	LockTS          uint64
	DeadlockKeyHash uint64
}

func (e ErrDeadlock) Error() string {
	return "deadlock"
}

type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}
