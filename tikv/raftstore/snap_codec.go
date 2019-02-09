package raftstore

import (
	"github.com/coocood/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

const (
	shortValuePrefix = 'v'
	shortValueMaxLen = 64
)

var (
	errBadLockFormat   = errors.New("bad format lock data")
	errInvalidSnapshot = errors.New("invalid snapshot")
	errBadKeyPrefix    = errors.New("bad key prefix")
)

type writeCFValue struct {
	writeType  byte
	startTS    uint64
	shortValue []byte
}

func decodeRocksDBSSTKey(k []byte) (key []byte, ts uint64, err error) {
	if k[0] != DataPrefix {
		return nil, 0, errors.WithStack(errBadKeyPrefix)
	}
	encodedKey := k[1 : len(k)-8]
	tsBytes := k[len(k)-8:]
	_, ts, err = codec.DecodeUintDesc(tsBytes)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	_, key, err = codec.DecodeBytes(encodedKey, nil)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return key, ts, nil
}

func encodeRocksDBSSTKey(k []byte, ts uint64) []byte {
	const encGroupSize = 8
	encodedKeySize := (len(k)/encGroupSize + 1) * (encGroupSize + 1)
	buf := make([]byte, 0, 1+encodedKeySize+8)
	buf = append(buf, DataPrefix)
	buf = codec.EncodeBytes(buf, k)
	return codec.EncodeUintDesc(buf, ts)
}

func decodeWriteCFValue(b []byte) *writeCFValue {
	w := new(writeCFValue)
	w.writeType = b[0]
	b = b[1:]
	var err error
	b, w.startTS, err = codec.DecodeUvarint(b)
	y.Assert(err == nil)
	if len(b) == 0 {
		return w
	}
	flag := b[0]
	y.Assert(flag == shortValuePrefix)
	l := b[1]
	b = b[2:]
	y.Assert(int(l) == len(b))
	w.shortValue = b
	return w
}

func encodeWriteCFValue(v *writeCFValue) []byte {
	size := 1 + 8
	shortValLen := len(v.shortValue)
	if shortValLen > 0 {
		size += shortValLen + 2
	}
	data := make([]byte, 0, size)
	data = append(data, v.writeType)
	data = codec.EncodeUvarint(data, v.startTS)
	if shortValLen > 0 {
		data = append(data, shortValuePrefix, byte(shortValLen))
		data = append(data, v.shortValue...)
	}
	return data
}

type lockCFValue struct {
	lockType byte
	primary  []byte
	startTS  uint64
	ttl      uint64
	shortVal []byte
}

func decodeLockCFValue(b []byte) (*lockCFValue, error) {
	if len(b) == 0 {
		return nil, errBadLockFormat
	}
	lv := new(lockCFValue)
	lv.lockType = b[0]
	b = b[1:]
	var err error
	b, lv.primary, err = codec.DecodeCompactBytes(b)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	b, lv.startTS, err = codec.DecodeUvarint(b)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) > 0 {
		b, lv.ttl, err = codec.DecodeUvarint(b)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if len(b) == 0 {
		return lv, nil
	}
	flag, l := b[0], b[1]
	b = b[2:]
	y.Assert(flag == shortValuePrefix)
	y.Assert(int(l) == len(b))
	lv.shortVal = b
	return lv, nil
}

func encodeLockCFValue(v *lockCFValue, buf []byte) []byte {
	buf = append(buf, v.lockType)
	buf = codec.EncodeCompactBytes(buf, v.primary)
	buf = codec.EncodeUvarint(buf, v.startTS)
	buf = codec.EncodeUvarint(buf, v.ttl)
	if len(v.shortVal) > 0 {
		buf = append(buf, shortValuePrefix, byte(len(v.shortVal)))
		buf = append(buf, v.shortVal...)
	}
	return buf
}
