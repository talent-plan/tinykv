package kvstore

import "encoding/binary"

// Write is a representation of a committed write to backing storage.
// A serialized version is stored in the "write" CF of our engine when a write is committed. That allows Txn to find
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

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindDelete   WriteKind = 2
	WriteKindLock     WriteKind = 3
	WriteKindRollback WriteKind = 4
)
