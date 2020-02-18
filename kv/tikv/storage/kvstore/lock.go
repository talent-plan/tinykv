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
	Ts      uint64
	Ttl     uint64
	Kind    WriteKind
}

// Info creates a LockInfo object from a Lock object for key.
func (lock *Lock) Info(key []byte) *kvrpcpb.LockInfo {
	info := kvrpcpb.LockInfo{}
	info.Key = key
	info.LockVersion = lock.Ts
	info.PrimaryLock = lock.Primary
	info.LockTtl = lock.Ttl
	return &info
}

func (lock *Lock) ToBytes() []byte {
	buf := append(lock.Primary, byte(lock.Kind))
	buf = append(buf, make([]byte, 16)...)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+1:], lock.Ts)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+9:], lock.Ttl)
	return buf
}

// ParseLock attempts to parse a byte string into a Lock object.
func ParseLock(input []byte) (*Lock, error) {
	if len(input) <= 16 {
		return nil, fmt.Errorf("kvstore: error parsing lock, not enough input, found %d bytes", len(input))
	}

	primaryLen := len(input) - 17
	primary := input[:primaryLen]
	kind := WriteKind(input[primaryLen])
	ts := binary.BigEndian.Uint64(input[primaryLen+1:])
	ttl := binary.BigEndian.Uint64(input[primaryLen+9:])

	return &Lock{Primary: primary, Ts: ts, Ttl: ttl, Kind: kind}, nil
}

// IsLockedFor checks if lock locks key at txnStartTs.
func (lock *Lock) IsLockedFor(key []byte, txnStartTs uint64) bool {
	if lock == nil {
		return false
	}
	if txnStartTs == TsMax && bytes.Compare(key, lock.Primary) != 0 {
		return false
	}
	return lock.Ts <= txnStartTs
}

// LockedError occurs when a key or keys are locked. The protobuf representation of the locked keys is stored as Info.
type LockedError struct {
	Info []kvrpcpb.LockInfo
}

func (err *LockedError) Error() string {
	return fmt.Sprintf("storage: %d keys are locked", len(err.Info))
}

// KeyErrors converts a LockedError to an array of KeyErrors for sending to the client.
func (err *LockedError) KeyErrors() []*kvrpcpb.KeyError {
	var result []*kvrpcpb.KeyError
	for _, i := range err.Info {
		var ke kvrpcpb.KeyError
		ke.Locked = &i
		result = append(result, &ke)
	}
	return result
}
