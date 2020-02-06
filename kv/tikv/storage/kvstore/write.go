package kvstore

import (
	"encoding/binary"
	"fmt"
)

// Write is a representation of a committed write to backing storage.
// A serialized version is stored in the "write" CF of our engine when a write is committed. That allows MvccTxn to find
// the status of a key at a given timestamp.
type Write struct {
	StartTS uint64
	Kind    WriteKind
}

func (wr *Write) ToBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

func ParseWrite(value []byte) (*Write, error) {
	if len(value) != 9 {
		return nil, fmt.Errorf("kvstore/write/ParseWrite: value is incorrect length, expected 9, found %d", len(value))
	}
	kind := value[0]
	startTs := binary.BigEndian.Uint64(value[1:])

	return &Write{startTs, WriteKind(kind)}, nil
}

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindDelete   WriteKind = 2
	WriteKindLock     WriteKind = 3
	WriteKindRollback WriteKind = 4
)
