package mvcc

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

type WriteType = byte

const (
	WriteTypeLock     WriteType = 'L'
	WriteTypeRollback WriteType = 'R'
	WriteTypeDelete   WriteType = 'D'
	WriteTypePut      WriteType = 'P'
)

type WriteCFValue struct {
	Type     byte
	StartTS  uint64
	ShortVal []byte
}

var invalidWriteCFValue = errors.New("")

func ParseWriteCFValue(data []byte) (wv WriteCFValue, err error) {
	if len(data) == 0 {
		err = invalidWriteCFValue
		return
	}
	wv.Type = data[0]
	wv.ShortVal, wv.StartTS, err = codec.DecodeUvarint(data[1:])
	return
}
