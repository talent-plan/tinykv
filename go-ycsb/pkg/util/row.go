// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/binary"

	"github.com/pingcap/errors"
)

// EncodeRow encodes row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, ...
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
// It is a simplified and specialized version of `github.com/pingcap/tidb/tablecodec.EncodeRow`.
func EncodeRow(cols [][]byte, colIDs []int64, valBuf []byte) ([]byte, error) {
	if len(cols) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: cols and colIDs count not match %d vs %d", len(cols), len(colIDs))
	}
	valBuf = valBuf[:0]
	if len(cols) == 0 {
		return append(valBuf, 0), nil
	}
	for i := range cols {
		valBuf = encodeInt64(valBuf, colIDs[i])
		valBuf = encodeBytes(valBuf, cols[i])
	}
	return valBuf, nil
}

const (
	compactBytesFlag byte = 2
	varintFlag       byte = 8
)

func encodeInt64(b []byte, v int64) []byte {
	b = append(b, varintFlag)
	return appendVarint(b, v)
}

func encodeBytes(b []byte, v []byte) []byte {
	b = append(b, compactBytesFlag)
	b = appendVarint(b, int64(len(v)))
	return append(b, v...)
}

func appendVarint(b []byte, v int64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeRow decodes a byte slice into columns.
// Row layout: colID1, value1, colID2, value2, .....
// It is a simplified and specialized version of `github.com/pingcap/tidb/tablecodec.DecodeRow`.
func DecodeRow(b []byte) (map[int64][]byte, error) {
	row := make(map[int64][]byte)
	if len(b) == 0 {
		return row, nil
	}
	if len(b) == 1 && b[0] == 0 {
		return row, nil
	}
	for len(b) > 0 {
		remain, rowID, err := decodeInt64(b)
		if err != nil {
			return row, err
		}
		var v []byte
		remain, v, err = decodeBytes(remain)
		if err != nil {
			return row, err
		}
		row[rowID] = v
		b = remain
	}
	return row, nil
}

func decodeInt64(b []byte) ([]byte, int64, error) {
	return decodeVarint(b[1:])
}

func decodeVarint(b []byte) ([]byte, int64, error) {
	v, n := binary.Varint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}

func decodeBytes(b []byte) ([]byte, []byte, error) {
	remain, n, err := decodeVarint(b[1:])
	if err != nil {
		return nil, nil, err
	}
	if int64(len(remain)) < n {
		return nil, nil, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return remain[n:], remain[:n], nil
}
