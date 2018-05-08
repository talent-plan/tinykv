package tikv

import (
	"github.com/dgraph-io/badger"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

type mvccValueType int

const (
	typePut mvccValueType = iota
	typeDelete
	typeRollback
)

type mvccValue struct {
	valueType mvccValueType
	startTS   uint64
	commitTS  uint64
	value     []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() []byte {
	buf := make([]byte, 0, len(v.value)+32)
	buf = codec.EncodeUint(buf, uint64(v.valueType))
	buf = codec.EncodeUint(buf, v.startTS)
	buf = codec.EncodeUint(buf, v.commitTS)
	buf = codec.EncodeCompactBytes(buf, v.value)
	return buf
}

func decodeValue(item *badger.Item) (mvVal mvccValue, err error) {
	val, err := item.Value()
	if err != nil {
		return mvVal, errors.Trace(err)
	}
	_, err = mvVal.UnmarshalBinary(val)
	if err != nil {
		return mvVal, errors.Trace(err)
	}
	return mvVal, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) ([]byte, error) {
	data, uVal, err := codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.valueType = mvccValueType(uVal)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.startTS = uVal
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.commitTS = uVal
	data, bVal, err := codec.DecodeCompactBytes(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.value = append([]byte{}, bVal...)
	return data, nil
}

type mvccLock struct {
	startTS    uint64
	primary    []byte
	value      []byte
	op         kvrpcpb.Op
	ttl        uint64
	rollbackTS uint64
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() []byte {
	buf := make([]byte, 0, len(l.primary)+len(l.value)+32)
	buf = codec.EncodeUint(buf, l.startTS)
	buf = codec.EncodeCompactBytes(buf, l.primary)
	buf = codec.EncodeCompactBytes(buf, l.value)
	buf = codec.EncodeUint(buf, uint64(l.op))
	buf = codec.EncodeUint(buf, l.ttl)
	buf = codec.EncodeUint(buf, l.rollbackTS)
	return buf
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) ([]byte, error) {
	data, uVal, err := codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.startTS = uVal
	data, bVal, err := codec.DecodeCompactBytes(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.primary = append([]byte{}, bVal...)
	data, bVal, err = codec.DecodeCompactBytes(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.value = append([]byte{}, bVal...)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.op = kvrpcpb.Op(uVal)
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.ttl = uVal
	data, uVal, err = codec.DecodeUint(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	l.rollbackTS = uVal
	return data, nil
}

func decodeLock(item *badger.Item) (mvccLock, error) {
	var lock mvccLock
	lockVal, err := item.Value()
	if err != nil {
		return lock, err
	}
	_, err = lock.UnmarshalBinary(lockVal)
	return lock, err
}

const (
	mixedLockFlag  byte = 1 << 0
	mixedValueFlag byte = 1 << 1
	mixedDelFlag   byte = 1 << 2
)

type mixedValue struct {
	mixedType byte
	val       mvccValue
	lock      mvccLock
}

func (mixed *mixedValue) hasLock() bool {
	return mixed.mixedType&mixedLockFlag > 0
}

func (mixed *mixedValue) hasValue() bool {
	return mixed.mixedType&mixedValueFlag > 0
}

func (mixed *mixedValue) isDelete() bool {
	return mixed.mixedType&mixedDelFlag > 0
}

func (mixed *mixedValue) unsetLock() {
	mixed.mixedType &= ^mixedLockFlag
}

func (mixed *mixedValue) UnmarshalBinary(data []byte) error {
	mixed.mixedType = data[0]
	data = data[1:]
	var err error
	if mixed.hasLock() {
		data, err = mixed.lock.UnmarshalBinary(data)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if mixed.hasValue() {
		data, err = mixed.val.UnmarshalBinary(data)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (mixed *mixedValue) MarshalBinary() []byte {
	buf := []byte{mixed.mixedType}
	if mixed.hasLock() {
		buf = append(buf, mixed.lock.MarshalBinary()...)
	}
	if mixed.hasValue() {
		buf = append(buf, mixed.val.MarshalBinary()...)
	}
	return buf
}

func decodeMixed(item *badger.Item) (mixedValue, error) {
	data, err := item.Value()
	if err != nil {
		return mixedValue{}, errors.Trace(err)
	}
	var mixed mixedValue
	err = mixed.UnmarshalBinary(data)
	return mixed, errors.Trace(err)
}

func encodeMVKey(key []byte) []byte {
	return codec.EncodeBytes(nil, key)
}

func encodeOldKeyFromMVKey(mvKey []byte, ts uint64) []byte {
	b := append([]byte{}, mvKey...)
	ret := codec.EncodeUintDesc(b, ts)
	ret[0]++
	return ret
}

func decodeRawKey(mvKey []byte) ([]byte, error) {
	_, rawKey, err := codec.DecodeBytes(mvKey, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rawKey, nil
}
