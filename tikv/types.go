package tikv

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/tidb/util/codec"
)

// decodeLock decodes data to lock, the primary and value is copied.
func decodeLock(data []byte) (l mvccLock) {
	l.mvccLockHdr = *(*mvccLockHdr)(unsafe.Pointer(&data[0]))
	cursor := mvccLockHdrSize
	var oldValLen int
	if l.hasOldVer {
		oldValLen = int(binary.LittleEndian.Uint32(data[cursor:]))
		cursor += 4
	}
	buf := append([]byte{}, data[cursor:]...)
	oldOff := len(buf) - oldValLen
	lockBuf := buf[:oldOff]
	l.primary = lockBuf[:l.primaryLen]
	l.value = lockBuf[l.primaryLen:]
	if l.hasOldVer {
		oldBuf := buf[oldOff:]
		l.oldMeta = oldBuf[:dbUserMetaLen]
		l.oldVal = oldBuf[dbUserMetaLen:]
	}
	return
}

type mvccLockHdr struct {
	startTS    uint64
	ttl        uint32
	op         uint8
	hasOldVer  bool
	primaryLen uint16
}

const mvccLockHdrSize = int(unsafe.Sizeof(mvccLockHdr{}))

type mvccLock struct {
	mvccLockHdr
	primary []byte
	value   []byte
	oldMeta dbUserMeta
	oldVal  []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() []byte {
	lockLen := mvccLockHdrSize + len(l.primary) + len(l.value)
	length := lockLen
	if l.hasOldVer {
		length += 4 + dbUserMetaLen + len(l.oldVal)
	}
	buf := make([]byte, length)
	hdr := (*mvccLockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = l.mvccLockHdr
	cursor := mvccLockHdrSize
	if l.hasOldVer {
		binary.LittleEndian.PutUint32(buf[cursor:], uint32(dbUserMetaLen+len(l.oldVal)))
		cursor += 4
	}
	copy(buf[cursor:], l.primary)
	cursor += len(l.primary)
	copy(buf[cursor:], l.value)
	cursor += len(l.value)
	if l.hasOldVer {
		copy(buf[cursor:], l.oldMeta)
		cursor += dbUserMetaLen
		copy(buf[cursor:], l.oldVal)
	}
	return buf
}

const (
	lockUserMetaNoneByte       = 0
	lockUserMetaRollbackByte   = 1
	lockUserMetaDeleteByte     = 2
	lockUserMetaRollbackGCByte = 3
)

var (
	lockUserMetaNone       = []byte{lockUserMetaNoneByte}
	lockUserMetaRollback   = []byte{lockUserMetaRollbackByte}
	lockUserMetaDelete     = []byte{lockUserMetaDeleteByte}
	lockUserMetaRollbackGC = []byte{lockUserMetaRollbackGCByte}
)

func encodeOldKey(key []byte, ts uint64) []byte {
	b := append([]byte{}, key...)
	ret := codec.EncodeUintDesc(b, ts)
	ret[0]++
	return ret
}

func encodeRollbackKey(buf, key []byte, ts uint64) []byte {
	buf = append(buf[:0], key...)
	buf = codec.EncodeUintDesc(buf, ts)
	return buf
}

func decodeRollbackTS(buf []byte) uint64 {
	tsBin := buf[len(buf)-8:]
	_, ts, err := codec.DecodeUintDesc(tsBin)
	if err != nil {
		panic(err)
	}
	return ts
}

type dbUserMeta []byte

const dbUserMetaLen = 16

func newDBUserMeta(startTS, commitTS uint64) dbUserMeta {
	m := make(dbUserMeta, 16)
	binary.LittleEndian.PutUint64(m, startTS)
	binary.LittleEndian.PutUint64(m[8:], commitTS)
	return m
}

func (m dbUserMeta) CommitTS() uint64 {
	return binary.LittleEndian.Uint64(m[8:])
}

func (m dbUserMeta) StartTS() uint64 {
	return binary.LittleEndian.Uint64(m[:8])
}
