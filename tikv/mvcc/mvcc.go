package mvcc

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/tidb/util/codec"
)

var defaultEndian = binary.LittleEndian

// DBUserMeta is the user meta used in DB.
type DBUserMeta []byte

const dbUserMetaLen = 16

// DecodeLock decodes data to lock, the primary and value is copied.
func DecodeLock(data []byte) (l MvccLock) {
	l.MvccLockHdr = *(*MvccLockHdr)(unsafe.Pointer(&data[0]))
	cursor := mvccLockHdrSize
	var oldValLen int
	if l.HasOldVer {
		oldValLen = int(defaultEndian.Uint32(data[cursor:]))
		cursor += 4
	}
	buf := append([]byte{}, data[cursor:]...)
	oldOff := len(buf) - oldValLen
	lockBuf := buf[:oldOff]
	l.Primary = lockBuf[:l.PrimaryLen]
	l.Value = lockBuf[l.PrimaryLen:]
	if l.HasOldVer {
		oldBuf := buf[oldOff:]
		l.OldMeta = oldBuf[:dbUserMetaLen]
		l.OldVal = oldBuf[dbUserMetaLen:]
	}
	return
}

// MvccLockHdr holds fixed size fields for MvccLock.
type MvccLockHdr struct {
	StartTS     uint64
	ForUpdateTS uint64
	MinCommitTS uint64
	TTL         uint32
	Op          uint8
	HasOldVer   bool
	PrimaryLen  uint16
}

const mvccLockHdrSize = int(unsafe.Sizeof(MvccLockHdr{}))

// MvccLock is the structure for MVCC lock.
type MvccLock struct {
	MvccLockHdr
	Primary []byte
	Value   []byte
	OldMeta DBUserMeta
	OldVal  []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *MvccLock) MarshalBinary() []byte {
	lockLen := mvccLockHdrSize + len(l.Primary) + len(l.Value)
	length := lockLen
	if l.HasOldVer {
		length += 4 + dbUserMetaLen + len(l.OldVal)
	}
	buf := make([]byte, length)
	hdr := (*MvccLockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = l.MvccLockHdr
	cursor := mvccLockHdrSize
	if l.HasOldVer {
		defaultEndian.PutUint32(buf[cursor:], uint32(dbUserMetaLen+len(l.OldVal)))
		cursor += 4
	}
	copy(buf[cursor:], l.Primary)
	cursor += len(l.Primary)
	copy(buf[cursor:], l.Value)
	cursor += len(l.Value)
	if l.HasOldVer {
		copy(buf[cursor:], l.OldMeta)
		cursor += dbUserMetaLen
		copy(buf[cursor:], l.OldVal)
	}
	return buf
}

// UserMeta value for lock.
const (
	LockUserMetaNoneByte       = 0
	LockUserMetaRollbackByte   = 1
	LockUserMetaDeleteByte     = 2
	LockUserMetaRollbackGCByte = 3
)

// UserMeta byte slices for lock.
var (
	LockUserMetaNone       = []byte{LockUserMetaNoneByte}
	LockUserMetaRollback   = []byte{LockUserMetaRollbackByte}
	LockUserMetaDelete     = []byte{LockUserMetaDeleteByte}
	LockUserMetaRollbackGC = []byte{LockUserMetaRollbackGCByte}
)

// EncodeOldKey encodes a latest key to an old key.
func EncodeOldKey(key []byte, ts uint64) []byte {
	b := append([]byte{}, key...)
	ret := codec.EncodeUintDesc(b, ts)
	ret[0]++
	return ret
}

// EncodeRollbackKey encodes a rollback key.
func EncodeRollbackKey(buf, key []byte, ts uint64) []byte {
	buf = append(buf[:0], key...)
	buf = codec.EncodeUintDesc(buf, ts)
	return buf
}

// DecodeRollbackTS decodes the TS in a rollback key.
func DecodeRollbackTS(buf []byte) uint64 {
	tsBin := buf[len(buf)-8:]
	_, ts, err := codec.DecodeUintDesc(tsBin)
	if err != nil {
		panic(err)
	}
	return ts
}

// NewDBUserMeta creates a new DBUserMeta.
func NewDBUserMeta(startTS, commitTS uint64) DBUserMeta {
	m := make(DBUserMeta, 16)
	defaultEndian.PutUint64(m, startTS)
	defaultEndian.PutUint64(m[8:], commitTS)
	return m
}

// CommitTS reads the commitTS from the DBUserMeta.
func (m DBUserMeta) CommitTS() uint64 {
	return defaultEndian.Uint64(m[8:])
}

// StartTS reads the startTS from the DBUserMeta.
func (m DBUserMeta) StartTS() uint64 {
	return defaultEndian.Uint64(m[:8])
}

// ToOldUserMeta converts a DBUserMeta to OldUserMeta.
// The old key has commitTS appended, we don't need to store it again in userMeta.
// We store the next commitTS which overwrite the old entry, it will be used in GCCompactionFilter.
func (m DBUserMeta) ToOldUserMeta(nextCommitTS uint64) OldUserMeta {
	o := OldUserMeta(m)
	defaultEndian.PutUint64(o[8:], nextCommitTS)
	return o
}

type OldUserMeta []byte

// StartTS reads the startTS from the OldUserMeta.
func (m OldUserMeta) StartTS() uint64 {
	return defaultEndian.Uint64(m[:8])
}

// NextCommitTS reads the next commitTS from the OldUserMeta.
func (m OldUserMeta) NextCommitTS() uint64 {
	return defaultEndian.Uint64(m[8:])
}
