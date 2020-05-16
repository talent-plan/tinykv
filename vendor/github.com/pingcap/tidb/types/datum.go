// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/hack"
)

// Kind constants.
const (
	KindNull          byte = 0
	KindInt64         byte = 1
	KindUint64        byte = 2
	KindFloat32       byte = 3
	KindFloat64       byte = 4
	KindString        byte = 5
	KindBytes         byte = 6
	KindBinaryLiteral byte = 7  // Used for BIT / HEX literals.
	KindMysqlBit      byte = 11 // Used for BIT table column values.
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindInterface     byte = 14
	KindMinNotNull    byte = 15
	KindMaxValue      byte = 16
	KindRaw           byte = 17
	KindMysqlJSON     byte = 18
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte        // datum kind.
	collation uint8       // collation can hold uint8 values.
	decimal   uint16      // decimal can hold uint16 values.
	length    uint32      // length can hold uint32 values.
	i         int64       // i can hold int64 uint64 float64 values.
	b         []byte      // b can hold string or []byte values.
	x         interface{} // x hold all other types.
}

// Copy deep copies a Datum.
func (d *Datum) Copy() *Datum {
	ret := *d
	if d.b != nil {
		ret.b = make([]byte, len(d.b))
		copy(ret.b, d.b)
	}
	return &ret
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	return d.k
}

// Collation gets the collation of the datum.
func (d *Datum) Collation() byte {
	return d.collation
}

// SetCollation sets the collation of the datum.
func (d *Datum) SetCollation(collation byte) {
	d.collation = collation
}

// Frac gets the frac of the datum.
func (d *Datum) Frac() int {
	return int(d.decimal)
}

// SetFrac sets the frac of the datum.
func (d *Datum) SetFrac(frac int) {
	d.decimal = uint16(frac)
}

// Length gets the length of the datum.
func (d *Datum) Length() int {
	return int(d.length)
}

// SetLength sets the length of the datum.
func (d *Datum) SetLength(l int) {
	d.length = uint32(l)
}

// IsNull checks if datum is null.
func (d *Datum) IsNull() bool {
	return d.k == KindNull
}

// GetInt64 gets int64 value.
func (d *Datum) GetInt64() int64 {
	return d.i
}

// SetInt64 sets int64 value.
func (d *Datum) SetInt64(i int64) {
	d.k = KindInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Datum) GetUint64() uint64 {
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Datum) SetUint64(i uint64) {
	d.k = KindUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Datum) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Datum) SetFloat64(f float64) {
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Datum) GetFloat32() float32 {
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetFloat32 sets float32 value.
func (d *Datum) SetFloat32(f float32) {
	d.k = KindFloat32
	d.i = int64(math.Float64bits(float64(f)))
}

// GetString gets string value.
func (d *Datum) GetString() string {
	return string(hack.String(d.b))
}

// SetString sets string value.
func (d *Datum) SetString(s string) {
	d.k = KindString
	sink(s)
	d.b = hack.Slice(s)
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	return d.b
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
}

// SetBytesAsString sets bytes value to datum as string type.
func (d *Datum) SetBytesAsString(b []byte) {
	d.k = KindString
	d.b = b
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() interface{} {
	return d.x
}

// SetInterface sets interface to datum.
func (d *Datum) SetInterface(x interface{}) {
	d.k = KindInterface
	d.x = x
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// SetMinNotNull sets datum to minNotNull value.
func (d *Datum) SetMinNotNull() {
	d.k = KindMinNotNull
	d.x = nil
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// GetMysqlBit gets MysqlBit value
func (d *Datum) GetMysqlBit() BinaryLiteral {
	return d.GetBinaryLiteral()
}

// SetBinaryLiteral sets Bit value
func (d *Datum) SetBinaryLiteral(b BinaryLiteral) {
	d.k = KindBinaryLiteral
	d.b = b
}

// SetMysqlBit sets MysqlBit value
func (d *Datum) SetMysqlBit(b BinaryLiteral) {
	d.k = KindMysqlBit
	d.b = b
}

// SetRaw sets raw value.
func (d *Datum) SetRaw(b []byte) {
	d.k = KindRaw
	d.b = b
}

// GetRaw gets raw value.
func (d *Datum) GetRaw() []byte {
	return d.b
}

// SetAutoID set the auto increment ID according to its int flag.
func (d *Datum) SetAutoID(id int64, flag uint) {
	if mysql.HasUnsignedFlag(flag) {
		d.SetUint64(uint64(id))
	} else {
		d.SetInt64(id)
	}
}

// String returns a human-readable description of Datum. It is intended only for debugging.
func (d Datum) String() string {
	var t string
	switch d.k {
	case KindNull:
		t = "KindNull"
	case KindInt64:
		t = "KindInt64"
	case KindUint64:
		t = "KindUint64"
	case KindFloat32:
		t = "KindFloat32"
	case KindFloat64:
		t = "KindFloat64"
	case KindString:
		t = "KindString"
	case KindBytes:
		t = "KindBytes"
	case KindBinaryLiteral:
		t = "KindBinaryLiteral"
	case KindMysqlBit:
		t = "KindMysqlBit"
	case KindMysqlSet:
		t = "KindMysqlSet"
	case KindMysqlJSON:
		t = "KindMysqlJSON"
	case KindMysqlTime:
		t = "KindMysqlTime"
	default:
		t = "Unknown"
	}
	v := d.GetValue()
	if b, ok := v.([]byte); ok && d.k == KindBytes {
		v = string(b)
	}
	return fmt.Sprintf("%v %v", t, v)
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() interface{} {
	switch d.k {
	case KindInt64:
		return d.GetInt64()
	case KindUint64:
		return d.GetUint64()
	case KindFloat32:
		return d.GetFloat32()
	case KindFloat64:
		return d.GetFloat64()
	case KindString:
		return d.GetString()
	case KindBytes:
		return d.GetBytes()
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral()
	default:
		return d.GetInterface()
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val interface{}) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x)
	case []byte:
		d.SetBytes(x)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	default:
		d.SetInterface(x)
	}
}

// CompareDatum compares datum to another datum.
// TODO: return error properly.
func (d *Datum) CompareDatum(sc *stmtctx.StatementContext, ad *Datum) (int, error) {
	if d.k == KindMysqlJSON && ad.k != KindMysqlJSON {
		cmp, err := ad.CompareDatum(sc, d)
		return cmp * -1, errors.Trace(err)
	}
	switch ad.k {
	case KindNull:
		if d.k == KindNull {
			return 0, nil
		}
		return 1, nil
	case KindMinNotNull:
		if d.k == KindNull {
			return -1, nil
		} else if d.k == KindMinNotNull {
			return 0, nil
		}
		return 1, nil
	case KindMaxValue:
		if d.k == KindMaxValue {
			return 0, nil
		}
		return -1, nil
	case KindInt64:
		return d.compareInt64(sc, ad.GetInt64())
	case KindUint64:
		return d.compareUint64(sc, ad.GetUint64())
	case KindFloat32, KindFloat64:
		return d.compareFloat64(sc, ad.GetFloat64())
	case KindString:
		return d.compareString(sc, ad.GetString())
	case KindBytes:
		return d.compareBytes(sc, ad.GetBytes())
	case KindBinaryLiteral, KindMysqlBit:
		return d.compareBinaryLiteral(sc, ad.GetBinaryLiteral())
	default:
		return 0, nil
	}
}

func (d *Datum) compareInt64(sc *stmtctx.StatementContext, i int64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return CompareInt64(d.i, i), nil
	case KindUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1, nil
		}
		return CompareInt64(d.i, i), nil
	default:
		return d.compareFloat64(sc, float64(i))
	}
}

func (d *Datum) compareUint64(sc *stmtctx.StatementContext, u uint64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1, nil
		}
		return CompareInt64(d.i, int64(u)), nil
	case KindUint64:
		return CompareUint64(d.GetUint64(), u), nil
	default:
		return d.compareFloat64(sc, float64(u))
	}
}

func (d *Datum) compareFloat64(sc *stmtctx.StatementContext, f float64) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return CompareFloat64(float64(d.i), f), nil
	case KindUint64:
		return CompareFloat64(float64(d.GetUint64()), f), nil
	case KindFloat32, KindFloat64:
		return CompareFloat64(d.GetFloat64(), f), nil
	case KindString, KindBytes:
		fVal, err := StrToFloat(sc, d.GetString())
		return CompareFloat64(fVal, f), errors.Trace(err)
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		fVal := float64(val)
		return CompareFloat64(fVal, f), errors.Trace(err)
	default:
		return -1, nil
	}
}

func (d *Datum) compareString(sc *stmtctx.StatementContext, s string) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		return CompareString(d.GetString(), s), nil
	case KindBinaryLiteral, KindMysqlBit:
		return CompareString(d.GetBinaryLiteral().ToString(), s), nil
	default:
		fVal, err := StrToFloat(sc, s)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Datum) compareBytes(sc *stmtctx.StatementContext, b []byte) (int, error) {
	str := string(hack.String(b))
	return d.compareString(sc, str)
}

func (d *Datum) compareBinaryLiteral(sc *stmtctx.StatementContext, b BinaryLiteral) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), b.ToString()), nil
	case KindBinaryLiteral, KindMysqlBit:
		return CompareString(d.GetBinaryLiteral().ToString(), b.ToString()), nil
	default:
		val, err := b.ToInt(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		result, err := d.compareFloat64(sc, float64(val))
		return result, errors.Trace(err)
	}
}

// ConvertTo converts a datum to the target field type.
func (d *Datum) ConvertTo(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	if d.k == KindNull {
		return Datum{}, nil
	}
	switch target.Tp { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		unsigned := mysql.HasUnsignedFlag(target.Flag)
		if unsigned {
			return d.convertToUint(sc, target)
		}
		return d.convertToInt(sc, target)
	case mysql.TypeFloat, mysql.TypeDouble:
		return d.convertToFloat(sc, target)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return d.convertToString(sc, target)
	case mysql.TypeBit:
		return d.convertToMysqlBit(sc, target)
	case mysql.TypeNull:
		return Datum{}, nil
	default:
		panic("should never happen")
	}
}

func (d *Datum) convertToFloat(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	var (
		f   float64
		ret Datum
		err error
	)
	switch d.k {
	case KindNull:
		return ret, nil
	case KindInt64:
		f = float64(d.GetInt64())
	case KindUint64:
		f = float64(d.GetUint64())
	case KindFloat32, KindFloat64:
		f = d.GetFloat64()
	case KindString, KindBytes:
		f, err = StrToFloat(sc, d.GetString())
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		f, err = float64(val), err1
	default:
		return invalidConv(d, target.Tp)
	}
	var err1 error
	f, err1 = ProduceFloatWithSpecifiedTp(f, target, sc)
	if err == nil && err1 != nil {
		err = err1
	}
	if target.Tp == mysql.TypeFloat {
		ret.SetFloat32(float32(f))
	} else {
		ret.SetFloat64(f)
	}
	return ret, errors.Trace(err)
}

// ProduceFloatWithSpecifiedTp produces a new float64 according to `flen` and `decimal`.
func ProduceFloatWithSpecifiedTp(f float64, target *FieldType, sc *stmtctx.StatementContext) (_ float64, err error) {
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
		f, err = TruncateFloat(f, target.Flen, target.Decimal)
		if err = sc.HandleOverflow(err, err); err != nil {
			return f, errors.Trace(err)
		}
	}
	if mysql.HasUnsignedFlag(target.Flag) && f < 0 {
		return 0, overflow(f, target.Tp)
	}
	return f, nil
}

func (d *Datum) convertToString(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	var ret Datum
	var s string
	switch d.k {
	case KindInt64:
		s = strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		s = strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat32:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case KindFloat64:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString, KindBytes:
		s = d.GetString()
	case KindBinaryLiteral, KindMysqlBit:
		s = d.GetBinaryLiteral().ToString()
	default:
		return invalidConv(d, target.Tp)
	}
	s, err := ProduceStrWithSpecifiedTp(s, target, sc, true)
	ret.SetString(s)
	if target.Charset == charset.CharsetBin {
		ret.k = KindBytes
	}
	return ret, errors.Trace(err)
}

// ProduceStrWithSpecifiedTp produces a new string according to `flen` and `chs`. Param `padZero` indicates
// whether we should pad `\0` for `binary(flen)` type.
func ProduceStrWithSpecifiedTp(s string, tp *FieldType, sc *stmtctx.StatementContext, padZero bool) (_ string, err error) {
	flen, chs := tp.Flen, tp.Charset
	if flen >= 0 {
		// Flen is the rune length, not binary length, for UTF8 charset, we need to calculate the
		// rune count and truncate to Flen runes if it is too long.
		if chs == charset.CharsetUTF8 || chs == charset.CharsetUTF8MB4 {
			characterLen := utf8.RuneCountInString(s)
			if characterLen > flen {
				// 1. If len(s) is 0 and flen is 0, truncateLen will be 0, don't truncate s.
				//    CREATE TABLE t (a char(0));
				//    INSERT INTO t VALUES (``);
				// 2. If len(s) is 10 and flen is 0, truncateLen will be 0 too, but we still need to truncate s.
				//    SELECT 1, CAST(1234 AS CHAR(0));
				// So truncateLen is not a suitable variable to determine to do truncate or not.
				var runeCount int
				var truncateLen int
				for i := range s {
					if runeCount == flen {
						truncateLen = i
						break
					}
					runeCount++
				}
				err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, characterLen)
				s = truncateStr(s, truncateLen)
			}
		} else if len(s) > flen {
			err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, len(s))
			s = truncateStr(s, flen)
		} else if tp.Tp == mysql.TypeString && IsBinaryStr(tp) && len(s) < flen && padZero {
			padding := make([]byte, flen-len(s))
			s = string(append([]byte(s), padding...))
		}
	}
	return s, errors.Trace(sc.HandleTruncate(err))
}

func (d *Datum) convertToInt(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	i64, err := d.toSignedInteger(sc, target.Tp)
	return NewIntDatum(i64), errors.Trace(err)
}

func (d *Datum) convertToUint(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	tp := target.Tp
	upperBound := IntergerUnsignedUpperBound(tp)
	var (
		val uint64
		err error
		ret Datum
	)
	switch d.k {
	case KindInt64:
		val, err = ConvertIntToUint(sc, d.GetInt64(), upperBound, tp)
	case KindUint64:
		val, err = ConvertUintToUint(d.GetUint64(), upperBound, tp)
	case KindFloat32, KindFloat64:
		val, err = ConvertFloatToUint(sc, d.GetFloat64(), upperBound, tp)
	case KindString, KindBytes:
		uval, err1 := StrToUint(sc, d.GetString())
		if err1 != nil && ErrOverflow.Equal(err1) && !sc.ShouldIgnoreOverflowError() {
			return ret, errors.Trace(err1)
		}
		val, err = ConvertUintToUint(uval, upperBound, tp)
		if err != nil {
			return ret, errors.Trace(err)
		}
		err = err1
	case KindBinaryLiteral, KindMysqlBit:
		val, err = d.GetBinaryLiteral().ToInt(sc)
	default:
		return invalidConv(d, target.Tp)
	}
	ret.SetUint64(val)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlBit(sc *stmtctx.StatementContext, target *FieldType) (Datum, error) {
	var ret Datum
	var uintValue uint64
	var err error
	switch d.k {
	case KindString, KindBytes:
		uintValue, err = BinaryLiteral(d.b).ToInt(sc)
	case KindInt64:
		// if input kind is int64 (signed), when trans to bit, we need to treat it as unsigned
		d.k = KindUint64
		fallthrough
	default:
		uintDatum, err1 := d.convertToUint(sc, target)
		uintValue, err = uintDatum.GetUint64(), err1
	}
	if target.Flen < 64 && uintValue >= 1<<(uint64(target.Flen)) {
		return Datum{}, errors.Trace(ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.Flen))
	}
	byteSize := (target.Flen + 7) >> 3
	ret.SetMysqlBit(NewBinaryLiteralFromUint(uintValue, byteSize))
	return ret, errors.Trace(err)
}

// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Datum) ToBool(sc *stmtctx.StatementContext) (int64, error) {
	var err error
	isZero := false
	switch d.Kind() {
	case KindInt64:
		isZero = d.GetInt64() == 0
	case KindUint64:
		isZero = d.GetUint64() == 0
	case KindFloat32:
		isZero = RoundFloat(d.GetFloat64()) == 0
	case KindFloat64:
		isZero = RoundFloat(d.GetFloat64()) == 0
	case KindString, KindBytes:
		iVal, err1 := StrToInt(sc, d.GetString())
		isZero, err = iVal == 0, err1
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		isZero, err = val == 0, err1
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	var ret int64
	if isZero {
		ret = 0
	} else {
		ret = 1
	}
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

// ToInt64 converts to a int64.
func (d *Datum) ToInt64(sc *stmtctx.StatementContext) (int64, error) {
	return d.toSignedInteger(sc, mysql.TypeLonglong)
}

func (d *Datum) toSignedInteger(sc *stmtctx.StatementContext, tp byte) (int64, error) {
	lowerBound := IntergerSignedLowerBound(tp)
	upperBound := IntergerSignedUpperBound(tp)
	switch d.Kind() {
	case KindInt64:
		return ConvertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case KindUint64:
		return ConvertUintToInt(d.GetUint64(), upperBound, tp)
	case KindFloat32:
		return ConvertFloatToInt(float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case KindFloat64:
		return ConvertFloatToInt(d.GetFloat64(), lowerBound, upperBound, tp)
	case KindString, KindBytes:
		iVal, err := StrToInt(sc, d.GetString())
		iVal, err2 := ConvertIntToInt(iVal, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return iVal, errors.Trace(err)
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		return int64(val), errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Datum) ToFloat64(sc *stmtctx.StatementContext) (float64, error) {
	switch d.Kind() {
	case KindInt64:
		return float64(d.GetInt64()), nil
	case KindUint64:
		return float64(d.GetUint64()), nil
	case KindFloat32:
		return float64(d.GetFloat32()), nil
	case KindFloat64:
		return d.GetFloat64(), nil
	case KindString:
		return StrToFloat(sc, d.GetString())
	case KindBytes:
		return StrToFloat(sc, string(d.GetBytes()))
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		return float64(val), errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", d.GetValue(), d.GetValue())
	}
}

// ToString gets the string representation of the datum.
func (d *Datum) ToString() (string, error) {
	switch d.Kind() {
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case KindFloat32:
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case KindString:
		return d.GetString(), nil
	case KindBytes:
		return d.GetString(), nil
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral().ToString(), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// ToBytes gets the bytes representation of the datum.
func (d *Datum) ToBytes() ([]byte, error) {
	switch d.k {
	case KindString, KindBytes:
		return d.GetBytes(), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []byte(str), nil
	}
}

func invalidConv(d *Datum, tp byte) (Datum, error) {
	return Datum{}, errors.Errorf("cannot convert datum from %s to type %s.", KindStr(d.Kind()), TypeStr(tp))
}

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in interface{}) (d Datum) {
	switch x := in.(type) {
	case []interface{}:
		d.SetValue(MakeDatums(x...))
	default:
		d.SetValue(in)
	}
	return d
}

// NewIntDatum creates a new Datum from an int64 value.
func NewIntDatum(i int64) (d Datum) {
	d.SetInt64(i)
	return d
}

// NewUintDatum creates a new Datum from an uint64 value.
func NewUintDatum(i uint64) (d Datum) {
	d.SetUint64(i)
	return d
}

// NewBytesDatum creates a new Datum from a byte slice.
func NewBytesDatum(b []byte) (d Datum) {
	d.SetBytes(b)
	return d
}

// NewStringDatum creates a new Datum from a string.
func NewStringDatum(s string) (d Datum) {
	d.SetString(s)
	return d
}

// NewFloat64Datum creates a new Datum from a float64 value.
func NewFloat64Datum(f float64) (d Datum) {
	d.SetFloat64(f)
	return d
}

// NewFloat32Datum creates a new Datum from a float32 value.
func NewFloat32Datum(f float32) (d Datum) {
	d.SetFloat32(f)
	return d
}

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...interface{}) []Datum {
	datums := make([]Datum, len(args))
	for i, v := range args {
		datums[i] = NewDatum(v)
	}
	return datums
}

// MinNotNullDatum returns a datum represents minimum not null value.
func MinNotNullDatum() Datum {
	return Datum{k: KindMinNotNull}
}

// MaxValueDatum returns a datum represents max value.
func MaxValueDatum() Datum {
	return Datum{k: KindMaxValue}
}

// EqualDatums compare if a and b contains the same datum values.
func EqualDatums(sc *stmtctx.StatementContext, a []Datum, b []Datum) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	if a == nil && b == nil {
		return true, nil
	}
	if a == nil || b == nil {
		return false, nil
	}
	for i, ai := range a {
		v, err := ai.CompareDatum(sc, &b[i])
		if err != nil {
			return false, errors.Trace(err)
		}
		if v != 0 {
			return false, nil
		}
	}
	return true, nil
}

// SortDatums sorts a slice of datum.
func SortDatums(sc *stmtctx.StatementContext, datums []Datum) error {
	sorter := datumsSorter{datums: datums, sc: sc}
	sort.Sort(&sorter)
	return sorter.err
}

type datumsSorter struct {
	datums []Datum
	sc     *stmtctx.StatementContext
	err    error
}

func (ds *datumsSorter) Len() int {
	return len(ds.datums)
}

func (ds *datumsSorter) Less(i, j int) bool {
	cmp, err := ds.datums[i].CompareDatum(ds.sc, &ds.datums[j])
	if err != nil {
		ds.err = errors.Trace(err)
		return true
	}
	return cmp < 0
}

func (ds *datumsSorter) Swap(i, j int) {
	ds.datums[i], ds.datums[j] = ds.datums[j], ds.datums[i]
}

func handleTruncateError(sc *stmtctx.StatementContext, err error) error {
	if sc.IgnoreTruncate {
		return nil
	}
	if !sc.TruncateAsWarning {
		return err
	}
	sc.AppendWarning(err)
	return nil
}

// DatumsToString converts several datums to formatted string.
func DatumsToString(datums []Datum, handleSpecialValue bool) (string, error) {
	strs := make([]string, 0, len(datums))
	for _, datum := range datums {
		if handleSpecialValue {
			switch datum.Kind() {
			case KindNull:
				strs = append(strs, "NULL")
				continue
			case KindMinNotNull:
				strs = append(strs, "-inf")
				continue
			case KindMaxValue:
				strs = append(strs, "+inf")
				continue
			}
		}
		str, err := datum.ToString()
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, str)
	}
	size := len(datums)
	if size > 1 {
		strs[0] = "(" + strs[0]
		strs[size-1] = strs[size-1] + ")"
	}
	return strings.Join(strs, ", "), nil
}

// DatumsToStrNoErr converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
func DatumsToStrNoErr(datums []Datum) string {
	str, err := DatumsToString(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// CloneDatum returns a new copy of the datum.
// TODO: Abandon this function.
func CloneDatum(datum Datum) Datum {
	return *datum.Copy()
}

// CloneRow deep copies a Datum slice.
func CloneRow(dr []Datum) []Datum {
	c := make([]Datum, len(dr))
	for i, d := range dr {
		c[i] = *d.Copy()
	}
	return c
}
