// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// Row represents a row of data, can be used to access values.
type Row struct {
	c   *Chunk
	idx int
}

// Chunk returns the Chunk which the row belongs to.
func (r Row) Chunk() *Chunk {
	return r.c
}

// IsEmpty returns true if the Row is empty.
func (r Row) IsEmpty() bool {
	return r == Row{}
}

// Idx returns the row index of Chunk.
func (r Row) Idx() int {
	return r.idx
}

// Len returns the number of values in the row.
func (r Row) Len() int {
	return r.c.NumCols()
}

// GetInt64 returns the int64 value with the colIdx.
func (r Row) GetInt64(colIdx int) int64 {
	return r.c.columns[colIdx].GetInt64(r.idx)
}

// GetUint64 returns the uint64 value with the colIdx.
func (r Row) GetUint64(colIdx int) uint64 {
	return r.c.columns[colIdx].GetUint64(r.idx)
}

// GetFloat32 returns the float32 value with the colIdx.
func (r Row) GetFloat32(colIdx int) float32 {
	return r.c.columns[colIdx].GetFloat32(r.idx)
}

// GetFloat64 returns the float64 value with the colIdx.
func (r Row) GetFloat64(colIdx int) float64 {
	return r.c.columns[colIdx].GetFloat64(r.idx)
}

// GetString returns the string value with the colIdx.
func (r Row) GetString(colIdx int) string {
	return r.c.columns[colIdx].GetString(r.idx)
}

// GetBytes returns the bytes value with the colIdx.
func (r Row) GetBytes(colIdx int) []byte {
	return r.c.columns[colIdx].GetBytes(r.idx)
}

// GetDatumRow converts chunk.Row to types.DatumRow.
// Keep in mind that GetDatumRow has a reference to r.c, which is a chunk,
// this function works only if the underlying chunk is valid or unchanged.
func (r Row) GetDatumRow(fields []*types.FieldType) []types.Datum {
	datumRow := make([]types.Datum, 0, r.c.NumCols())
	for colIdx := 0; colIdx < r.c.NumCols(); colIdx++ {
		datum := r.GetDatum(colIdx, fields[colIdx])
		datumRow = append(datumRow, datum)
	}
	return datumRow
}

// GetDatum implements the chunk.Row interface.
func (r Row) GetDatum(colIdx int, tp *types.FieldType) types.Datum {
	var d types.Datum
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if !r.IsNull(colIdx) {
			if mysql.HasUnsignedFlag(tp.Flag) {
				d.SetUint64(r.GetUint64(colIdx))
			} else {
				d.SetInt64(r.GetInt64(colIdx))
			}
		}
	case mysql.TypeYear:
		// FIXBUG: because insert type of TypeYear is definite int64, so we regardless of the unsigned flag.
		if !r.IsNull(colIdx) {
			d.SetInt64(r.GetInt64(colIdx))
		}
	case mysql.TypeFloat:
		if !r.IsNull(colIdx) {
			d.SetFloat32(r.GetFloat32(colIdx))
		}
	case mysql.TypeDouble:
		if !r.IsNull(colIdx) {
			d.SetFloat64(r.GetFloat64(colIdx))
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if !r.IsNull(colIdx) {
			d.SetBytes(r.GetBytes(colIdx))
		}
	}
	return d
}

// GetRaw returns the underlying raw bytes with the colIdx.
func (r Row) GetRaw(colIdx int) []byte {
	return r.c.columns[colIdx].GetRaw(r.idx)
}

// IsNull returns if the datum in the chunk.Row is null.
func (r Row) IsNull(colIdx int) bool {
	return r.c.columns[colIdx].IsNull(r.idx)
}

// CopyConstruct creates a new row and copies this row's data into it.
func (r Row) CopyConstruct() Row {
	newChk := renewWithCapacity(r.c, 1, 1)
	newChk.AppendRow(r)
	return newChk.GetRow(0)
}
