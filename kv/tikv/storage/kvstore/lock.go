package kvstore

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Lock struct {
	Primary []byte
	TS      uint64
}

// Info creates a LockInfo object from a Lock object.
func (lock *Lock) Info() *kvrpcpb.LockInfo {
	info := kvrpcpb.LockInfo{}
	info.LockVersion = lock.TS
	info.PrimaryLock = lock.Primary
	return &info
}

func (lock *Lock) ToBytes() []byte {
	buf := append(lock.Primary, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[len(lock.Primary):], lock.TS)
	return buf
}

// ParseLock attempts to parse a byte string into a Lock object.
func ParseLock(input []byte) (*Lock, error) {
	if len(input) <= 8 {
		return nil, fmt.Errorf("kvstore: error parsing lock, not enough input, found %d bytes", len(input))
	}

	primaryLen := len(input) - 8
	primary := input[:primaryLen]
	ts := binary.BigEndian.Uint64(input[primaryLen:])

	return &Lock{primary, ts}, nil
}
