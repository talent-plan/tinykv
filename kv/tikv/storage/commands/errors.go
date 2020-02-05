package commands

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// LockedError occurs when a key or keys are locked. The protobuf representation of the locked keys is stored as info.
type LockedError struct {
	info []kvrpcpb.LockInfo
}

func (err *LockedError) Error() string {
	return fmt.Sprintf("storage: %d keys are locked", len(err.info))
}

type KeyError interface {
	keyErrors() []*kvrpcpb.KeyError
}

// keyErrors converts a LockedError to amn array of KeyErrors for sending to the client.
func (err *LockedError) keyErrors() []*kvrpcpb.KeyError {
	var result []*kvrpcpb.KeyError
	for _, i := range err.info {
		var ke kvrpcpb.KeyError
		ke.Locked = &i
		result = append(result, &ke)
	}
	return result
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

func (err *WriteConflict) keyErrors() []*kvrpcpb.KeyError {
	var result kvrpcpb.KeyError
	result.Conflict = &kvrpcpb.WriteConflict{
		StartTs:    err.startTS,
		ConflictTs: err.conflictTS,
		Key:        err.key,
		Primary:    err.primary,
	}
	return []*kvrpcpb.KeyError{&result}
}

// AlreadyExist occurs when a client tries to *insert* a value, but the key is already occupied.
type AlreadyExist struct {
	key []byte
}

func (err *AlreadyExist) Error() string {
	return fmt.Sprintf("storage: value already exists at key %v", err.key)
}

func (err *AlreadyExist) keyErrors() []*kvrpcpb.KeyError {
	var result kvrpcpb.KeyError
	result.AlreadyExist = &kvrpcpb.AlreadyExist{
		Key: err.key,
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
