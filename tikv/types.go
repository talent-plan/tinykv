package tikv

import (
	"encoding/binary"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
)

type mvccValueHdr struct {
	startTS  uint64
	commitTS uint64
}

const mvccValueHdrSize = int(unsafe.Sizeof(mvccValueHdr{}))

type mvccValue struct {
	mvccValueHdr
	value []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() []byte {
	buf := make([]byte, mvccValueHdrSize+len(v.value))
	hdr := (*mvccValueHdr)(unsafe.Pointer(&buf[0]))
	*hdr = v.mvccValueHdr
	copy(buf[mvccValueHdrSize:], v.value)
	return buf
}

func decodeValue(item *badger.Item) (v mvccValue, err error) {
	val, err := item.Value()
	if err != nil {
		return v, errors.Trace(err)
	}
	v.mvccValueHdr = *(*mvccValueHdr)(unsafe.Pointer(&val[0]))
	if len(val) > mvccValueHdrSize {
		v.value = append(v.value[:0], val[mvccValueHdrSize:]...)
	}
	return v, nil
}

func decodeValueTo(item *badger.Item, v *mvccValue) error {
	val, err := item.Value()
	if err != nil {
		return errors.Trace(err)
	}
	v.mvccValueHdr = *(*mvccValueHdr)(unsafe.Pointer(&val[0]))
	v.value = val[mvccValueHdrSize:]
	return nil
}

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
		l.oldVal.mvccValueHdr = *(*mvccValueHdr)(unsafe.Pointer(&oldBuf[0]))
		l.oldVal.value = oldBuf[mvccValueHdrSize:]
	}
	return
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) {
	v.mvccValueHdr = *(*mvccValueHdr)(unsafe.Pointer(&data[0]))
	if len(data) > mvccValueHdrSize {
		v.value = append(v.value[:0], data[mvccValueHdrSize:]...)
	}
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
	oldVal  mvccValue
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() []byte {
	lockLen := mvccLockHdrSize + len(l.primary) + len(l.value)
	length := lockLen
	if l.oldVal.commitTS > 0 {
		length += 4 + mvccValueHdrSize + len(l.oldVal.value)
	}
	buf := make([]byte, length)
	hdr := (*mvccLockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = l.mvccLockHdr
	cursor := mvccLockHdrSize
	if l.hasOldVer {
		binary.LittleEndian.PutUint32(buf[cursor:], uint32(mvccValueHdrSize+len(l.oldVal.value)))
		cursor += 4
	}
	copy(buf[cursor:], l.primary)
	cursor += len(l.primary)
	copy(buf[cursor:], l.value)
	cursor += len(l.value)
	if l.hasOldVer {
		oldValHdr := (*mvccValueHdr)(unsafe.Pointer(&buf[cursor]))
		*oldValHdr = l.oldVal.mvccValueHdr
		cursor += mvccValueHdrSize
		copy(buf[cursor:], l.oldVal.value)
	}
	return buf
}

func lockToValue(lock mvccLock, commitTS uint64) (mvVal mvccValue) {
	mvVal.startTS = lock.startTS
	mvVal.commitTS = commitTS
	mvVal.value = lock.value
	return
}

const (
	userMetaNone       byte = 0
	userMetaRollback   byte = 1
	userMetaDelete     byte = 2
	userMetaRollbackGC byte = 3
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
