package kvstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

const TsMax uint64 = ^uint64(0)

type Lock struct {
	Primary []byte
	TS      uint64
	Kind    WriteKind
}

// Info creates a LockInfo object from a Lock object for key.
func (lock *Lock) Info(key []byte) *kvrpcpb.LockInfo {
	info := kvrpcpb.LockInfo{}
	info.Key = key
	info.LockVersion = lock.TS
	info.PrimaryLock = lock.Primary
	info.LockType = lock.Kind.ToProto()
	return &info
}

func (lock *Lock) ToBytes() []byte {
	buf := append(lock.Primary, byte(lock.Kind), 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+1:], lock.TS)
	return buf
}

// ParseLock attempts to parse a byte string into a Lock object.
func ParseLock(input []byte) (*Lock, error) {
	if len(input) <= 8 {
		return nil, fmt.Errorf("kvstore: error parsing lock, not enough input, found %d bytes", len(input))
	}

	primaryLen := len(input) - 9
	primary := input[:primaryLen]
	kind := WriteKind(input[primaryLen])
	ts := binary.BigEndian.Uint64(input[primaryLen+1:])

	return &Lock{primary, ts, kind}, nil
}

// IsLockedFor checks if lock locks key at txnStartTs.
func (lock *Lock) IsLockedFor(key []byte, txnStartTs uint64) bool {
	if lock == nil {
		return false
	}
	if txnStartTs == TsMax && bytes.Compare(key, lock.Primary) != 0 {
		return false
	}
	return lock.TS <= txnStartTs
}
