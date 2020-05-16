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
	"sort"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// CompareFunc is a function to compare the two values in Row, the two columns must have the same type.
type CompareFunc = func(l Row, lCol int, r Row, rCol int) int

// GetCompareFunc gets a compare function for the field type.
func GetCompareFunc(tp *types.FieldType) CompareFunc {
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(tp.Flag) {
			return cmpUint64
		}
		return cmpInt64
	case mysql.TypeFloat:
		return cmpFloat32
	case mysql.TypeDouble:
		return cmpFloat64
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return cmpString
	}
	return nil
}

func cmpNull(lNull, rNull bool) int {
	if lNull && rNull {
		return 0
	}
	if lNull {
		return -1
	}
	return 1
}

func cmpInt64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareInt64(l.GetInt64(lCol), r.GetInt64(rCol))
}

func cmpUint64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareUint64(l.GetUint64(lCol), r.GetUint64(rCol))
}

func cmpString(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareString(l.GetString(lCol), r.GetString(rCol))
}

func cmpFloat32(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(float64(l.GetFloat32(lCol)), float64(r.GetFloat32(rCol)))
}

func cmpFloat64(l Row, lCol int, r Row, rCol int) int {
	lNull, rNull := l.IsNull(lCol), r.IsNull(rCol)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(l.GetFloat64(lCol), r.GetFloat64(rCol))
}

// Compare compares the value with ad.
func Compare(row Row, colIdx int, ad *types.Datum) int {
	switch ad.Kind() {
	case types.KindNull:
		if row.IsNull(colIdx) {
			return 0
		}
		return 1
	case types.KindMinNotNull:
		if row.IsNull(colIdx) {
			return -1
		}
		return 1
	case types.KindMaxValue:
		return -1
	case types.KindInt64:
		return types.CompareInt64(row.GetInt64(colIdx), ad.GetInt64())
	case types.KindUint64:
		return types.CompareUint64(row.GetUint64(colIdx), ad.GetUint64())
	case types.KindFloat32:
		return types.CompareFloat64(float64(row.GetFloat32(colIdx)), float64(ad.GetFloat32()))
	case types.KindFloat64:
		return types.CompareFloat64(row.GetFloat64(colIdx), ad.GetFloat64())
	case types.KindString, types.KindBytes:
		return types.CompareString(row.GetString(colIdx), ad.GetString())
	default:
		return 0
	}
}

// LowerBound searches on the non-decreasing Column colIdx,
// returns the smallest index i such that the value at row i is not less than `d`.
func (c *Chunk) LowerBound(colIdx int, d *types.Datum) (index int, match bool) {
	index = sort.Search(c.NumRows(), func(i int) bool {
		cmp := Compare(c.GetRow(i), colIdx, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	return
}

// UpperBound searches on the non-decreasing Column colIdx,
// returns the smallest index i such that the value at row i is larger than `d`.
func (c *Chunk) UpperBound(colIdx int, d *types.Datum) int {
	return sort.Search(c.NumRows(), func(i int) bool {
		return Compare(c.GetRow(i), colIdx, d) > 0
	})
}
