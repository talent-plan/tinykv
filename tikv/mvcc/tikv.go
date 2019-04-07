package mvcc

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

type WriteType = byte

const (
	WriteTypeLock     WriteType = 'L'
	WriteTypeRollback WriteType = 'R'
	WriteTypeDelete   WriteType = 'D'
	WriteTypePut      WriteType = 'P'
)

type WriteCFValue struct {
	Type     WriteType
	StartTS  uint64
	ShortVal []byte
}

var invalidWriteCFValue = errors.New("invalid write CF value")

func ParseWriteCFValue(data []byte) (wv WriteCFValue, err error) {
	if len(data) == 0 {
		err = invalidWriteCFValue
		return
	}
	wv.Type = data[0]
	switch wv.Type {
	case WriteTypePut, WriteTypeDelete, WriteTypeLock, WriteTypeRollback:
	default:
		err = invalidWriteCFValue
		return
	}
	wv.ShortVal, wv.StartTS, err = codec.DecodeUvarint(data[1:])
	return
}

type LockType = byte

const (
	LockTypePut    LockType = 'P'
	LockTypeDelete LockType = 'D'
	LockTypeLock   LockType = 'L'
)

var invalidLockCFValue = errors.New("invalid lock CF value")

func ParseLockCFValue(data []byte) (lock MvccLock, err error) {
	if len(data) == 0 {
		err = invalidLockCFValue
		return
	}
	switch data[0] {
	case LockTypePut:
		lock.Op = byte(kvrpcpb.Op_Put)
	case LockTypeDelete:
		lock.Op = byte(kvrpcpb.Op_Del)
	case LockTypeLock:
		lock.Op = byte(kvrpcpb.Op_Lock)
	default:
		err = invalidLockCFValue
		return
	}
	data, lock.Primary, err = codec.DecodeCompactBytes(data[1:])
	if err != nil {
		return
	}
	lock.PrimaryLen = uint16(len(lock.Primary))
	data, lock.StartTS, err = codec.DecodeUvarint(data)
	if err != nil || len(data) == 0 {
		return
	}
	var ttl uint64
	data, ttl, err = codec.DecodeUvarint(data)
	lock.TTL = uint32(ttl)
	if err != nil || len(data) == 0 {
		return
	}
	shortValLen := int(data[0])
	lock.Value = data[1:]
	if shortValLen != len(lock.Value) {
		err = invalidLockCFValue
	}
	return
}
