package tikv

import (
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
	buf := append([]byte{}, data[mvccLockHdrSize:]...)
	l.primary = buf[:l.primaryLen]
	l.value = buf[l.primaryLen:]
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
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() []byte {
	buf := make([]byte, mvccLockHdrSize+len(l.primary)+len(l.value))
	hdr := (*mvccLockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = l.mvccLockHdr
	copy(buf[mvccLockHdrSize:], l.primary)
	copy(buf[mvccLockHdrSize+int(l.primaryLen):], l.value)
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
