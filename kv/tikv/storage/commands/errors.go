package commands

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type KeyError interface {
	KeyErrors() []*kvrpcpb.KeyError
}

// WriteConflict occurs when writes from two transactions conflict.
type WriteConflict struct {
	startTS    uint64
	conflictTS uint64
	key        []byte
	primary    []byte
}

func (err *WriteConflict) Error() string {
	return fmt.Sprintf("storage: write conflict at key %d", err.key)
}

func (err *WriteConflict) KeyErrors() []*kvrpcpb.KeyError {
	var result kvrpcpb.KeyError
	result.Conflict = &kvrpcpb.WriteConflict{
		StartTs:    err.startTS,
		ConflictTs: err.conflictTS,
		Key:        err.key,
		Primary:    err.primary,
	}
	return []*kvrpcpb.KeyError{&result}
}

// TODO duplicated from errors.go to avoid import cycle
func extractRegionError(err error) *errorpb.Error {
	if regionError, ok := err.(*inner_server.RegionError); ok {
		return regionError.RequestErr
	}
	return nil
}

type LockNotFound struct {
	key []byte
}

func (err *LockNotFound) Error() string {
	return fmt.Sprintf("storage: lock not found for %v", err.key)
}

func (err *LockNotFound) KeyErrors() []*kvrpcpb.KeyError {
	var result kvrpcpb.KeyError
	result.Retryable = fmt.Sprintf("lock not found for key %v", err.key)
	return []*kvrpcpb.KeyError{&result}
}

type Committed struct {
	key []byte
	ts  uint64
}

func (err *Committed) Error() string {
	return fmt.Sprintf("storage: key has already been committed: %v at %d", err.key, err.ts)
}

func (err *Committed) KeyErrors() []*kvrpcpb.KeyError {
	var result kvrpcpb.KeyError
	result.Abort = fmt.Sprintf("key has already been committed: %v at %d", err.key, err.ts)
	return []*kvrpcpb.KeyError{&result}
}
