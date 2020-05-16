// Copyright 2017 PingCAP, Inc.
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

package chunk

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
)

// MutRow represents a mutable Row.
// The underlying columns only contains one row and not exposed to the user.
type MutRow Row

// ToRow converts the MutRow to Row, so it can be used to read data.
func (mr MutRow) ToRow() Row {
	return Row(mr)
}

// Len returns the number of columns.
func (mr MutRow) Len() int {
	return len(mr.c.columns)
}

// MutRowFromValues creates a MutRow from a interface slice.
func MutRowFromValues(vals ...interface{}) MutRow {
	c := &Chunk{columns: make([]*Column, 0, len(vals))}
	for _, val := range vals {
		col := makeMutRowColumn(val)
		c.columns = append(c.columns, col)
	}
	return MutRow{c: c}
}

// MutRowFromDatums creates a MutRow from a datum slice.
func MutRowFromDatums(datums []types.Datum) MutRow {
	c := &Chunk{columns: make([]*Column, 0, len(datums))}
	for _, d := range datums {
		col := makeMutRowColumn(d.GetValue())
		c.columns = append(c.columns, col)
	}
	return MutRow{c: c, idx: 0}
}

// MutRowFromTypes creates a MutRow from a FieldType slice, each Column is initialized to zero value.
func MutRowFromTypes(types []*types.FieldType) MutRow {
	c := &Chunk{columns: make([]*Column, 0, len(types))}
	for _, tp := range types {
		col := makeMutRowColumn(zeroValForType(tp))
		c.columns = append(c.columns, col)
	}
	return MutRow{c: c, idx: 0}
}

func zeroValForType(tp *types.FieldType) interface{} {
	switch tp.Tp {
	case mysql.TypeFloat:
		return float32(0)
	case mysql.TypeDouble:
		return float64(0)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(tp.Flag) {
			return uint64(0)
		}
		return int64(0)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		return ""
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return []byte{}
	default:
		return nil
	}
}

func makeMutRowColumn(in interface{}) *Column {
	switch x := in.(type) {
	case nil:
		col := makeMutRowUint64Column(uint64(0))
		col.nullBitmap[0] = 0
		return col
	case int:
		return makeMutRowUint64Column(uint64(x))
	case int64:
		return makeMutRowUint64Column(uint64(x))
	case uint64:
		return makeMutRowUint64Column(x)
	case float64:
		return makeMutRowUint64Column(math.Float64bits(x))
	case float32:
		col := newMutRowFixedLenColumn(4)
		*(*uint32)(unsafe.Pointer(&col.data[0])) = math.Float32bits(x)
		return col
	case string:
		return makeMutRowBytesColumn(hack.Slice(x))
	case []byte:
		return makeMutRowBytesColumn(x)
	default:
		return nil
	}
}

func newMutRowFixedLenColumn(elemSize int) *Column {
	buf := make([]byte, elemSize+1)
	col := &Column{
		length:     1,
		elemBuf:    buf[:elemSize],
		data:       buf[:elemSize],
		nullBitmap: buf[elemSize:],
	}
	col.nullBitmap[0] = 1
	return col
}

func newMutRowVarLenColumn(valSize int) *Column {
	buf := make([]byte, valSize+1)
	col := &Column{
		length:     1,
		offsets:    []int64{0, int64(valSize)},
		data:       buf[:valSize],
		nullBitmap: buf[valSize:],
	}
	col.nullBitmap[0] = 1
	return col
}

func makeMutRowUint64Column(val uint64) *Column {
	col := newMutRowFixedLenColumn(8)
	*(*uint64)(unsafe.Pointer(&col.data[0])) = val
	return col
}

func makeMutRowBytesColumn(bin []byte) *Column {
	col := newMutRowVarLenColumn(len(bin))
	copy(col.data, bin)
	col.nullBitmap[0] = 1
	return col
}

// SetRow sets the MutRow with Row.
func (mr MutRow) SetRow(row Row) {
	for colIdx, rCol := range row.c.columns {
		mrCol := mr.c.columns[colIdx]
		if rCol.IsNull(row.idx) {
			mrCol.nullBitmap[0] = 0
			continue
		}
		elemLen := len(rCol.elemBuf)
		if elemLen > 0 {
			copy(mrCol.data, rCol.data[row.idx*elemLen:(row.idx+1)*elemLen])
		} else {
			setMutRowBytes(mrCol, rCol.data[rCol.offsets[row.idx]:rCol.offsets[row.idx+1]])
		}
		mrCol.nullBitmap[0] = 1
	}
}

// SetValues sets the MutRow with values.
func (mr MutRow) SetValues(vals ...interface{}) {
	for i, v := range vals {
		mr.SetValue(i, v)
	}
}

// SetValue sets the MutRow with colIdx and value.
func (mr MutRow) SetValue(colIdx int, val interface{}) {
	col := mr.c.columns[colIdx]
	if val == nil {
		col.nullBitmap[0] = 0
		return
	}
	switch x := val.(type) {
	case int:
		binary.LittleEndian.PutUint64(col.data, uint64(x))
	case int64:
		binary.LittleEndian.PutUint64(col.data, uint64(x))
	case uint64:
		binary.LittleEndian.PutUint64(col.data, x)
	case float64:
		binary.LittleEndian.PutUint64(col.data, math.Float64bits(x))
	case float32:
		binary.LittleEndian.PutUint32(col.data, math.Float32bits(x))
	case string:
		setMutRowBytes(col, hack.Slice(x))
	case []byte:
		setMutRowBytes(col, x)
	}
	col.nullBitmap[0] = 1
}

// SetDatums sets the MutRow with datum slice.
func (mr MutRow) SetDatums(datums ...types.Datum) {
	for i, d := range datums {
		mr.SetDatum(i, d)
	}
}

// SetDatum sets the MutRow with colIdx and datum.
func (mr MutRow) SetDatum(colIdx int, d types.Datum) {
	col := mr.c.columns[colIdx]
	if d.IsNull() {
		col.nullBitmap[0] = 0
		return
	}
	switch d.Kind() {
	case types.KindInt64, types.KindUint64, types.KindFloat64:
		binary.LittleEndian.PutUint64(mr.c.columns[colIdx].data, d.GetUint64())
	case types.KindFloat32:
		binary.LittleEndian.PutUint32(mr.c.columns[colIdx].data, math.Float32bits(d.GetFloat32()))
	case types.KindString, types.KindBytes:
		setMutRowBytes(col, d.GetBytes())
	default:
		mr.c.columns[colIdx] = makeMutRowColumn(d.GetValue())
	}
	col.nullBitmap[0] = 1
}

func setMutRowBytes(col *Column, bin []byte) {
	if len(col.data) >= len(bin) {
		col.data = col.data[:len(bin)]
	} else {
		buf := make([]byte, len(bin)+1)
		col.data = buf[:len(bin)]
		col.nullBitmap = buf[len(bin):]
	}
	copy(col.data, bin)
	col.offsets[1] = int64(len(bin))
}

// ShallowCopyPartialRow shallow copies the data of `row` to MutRow.
func (mr MutRow) ShallowCopyPartialRow(colIdx int, row Row) {
	for i, srcCol := range row.c.columns {
		dstCol := mr.c.columns[colIdx+i]
		if !srcCol.IsNull(row.idx) {
			// MutRow only contains one row, so we can directly set the whole byte.
			dstCol.nullBitmap[0] = 1
		} else {
			dstCol.nullBitmap[0] = 0
		}

		if srcCol.isFixed() {
			elemLen := len(srcCol.elemBuf)
			offset := row.idx * elemLen
			dstCol.data = srcCol.data[offset : offset+elemLen]
		} else {
			start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+1]
			dstCol.data = srcCol.data[start:end]
			dstCol.offsets[1] = int64(len(dstCol.data))
		}
	}
}
