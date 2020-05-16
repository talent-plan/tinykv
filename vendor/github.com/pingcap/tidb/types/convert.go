// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
)

func truncateStr(str string, flen int) string {
	if flen != UnspecifiedLength && len(str) > flen {
		str = str[:flen]
	}
	return str
}

// IntergerUnsignedUpperBound indicates the max uint64 values of different mysql types.
func IntergerUnsignedUpperBound(intType byte) uint64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MaxUint8
	case mysql.TypeShort:
		return math.MaxUint16
	case mysql.TypeInt24:
		return mysql.MaxUint24
	case mysql.TypeLong:
		return math.MaxUint32
	case mysql.TypeLonglong:
		return math.MaxUint64
	default:
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedUpperBound indicates the max int64 values of different mysql types.
func IntergerSignedUpperBound(intType byte) int64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MaxInt8
	case mysql.TypeShort:
		return math.MaxInt16
	case mysql.TypeInt24:
		return mysql.MaxInt24
	case mysql.TypeLong:
		return math.MaxInt32
	case mysql.TypeLonglong:
		return math.MaxInt64
	default:
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedLowerBound indicates the min int64 values of different mysql types.
func IntergerSignedLowerBound(intType byte) int64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MinInt8
	case mysql.TypeShort:
		return math.MinInt16
	case mysql.TypeInt24:
		return mysql.MinInt24
	case mysql.TypeLong:
		return math.MinInt32
	case mysql.TypeLonglong:
		return math.MinInt64
	default:
		panic("Input byte is not a mysql type")
	}
}

// ConvertFloatToInt converts a float64 value to a int value.
// `tp` is used in err msg, if there is overflow, this func will report err according to `tp`
func ConvertFloatToInt(fval float64, lowerBound, upperBound int64, tp byte) (int64, error) {
	val := RoundFloat(fval)
	if val < float64(lowerBound) {
		return lowerBound, overflow(val, tp)
	}

	if val >= float64(upperBound) {
		if val == float64(upperBound) {
			return upperBound, nil
		}
		return upperBound, overflow(val, tp)
	}
	return int64(val), nil
}

// ConvertIntToInt converts an int value to another int value of different precision.
func ConvertIntToInt(val int64, lowerBound int64, upperBound int64, tp byte) (int64, error) {
	if val < lowerBound {
		return lowerBound, overflow(val, tp)
	}

	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

// ConvertUintToInt converts an uint value to an int value.
func ConvertUintToInt(val uint64, upperBound int64, tp byte) (int64, error) {
	if val > uint64(upperBound) {
		return upperBound, overflow(val, tp)
	}

	return int64(val), nil
}

// ConvertIntToUint converts an int value to an uint value.
func ConvertIntToUint(sc *stmtctx.StatementContext, val int64, upperBound uint64, tp byte) (uint64, error) {
	if sc.ShouldClipToZero() && val < 0 {
		return 0, overflow(val, tp)
	}

	if uint64(val) > upperBound {
		return upperBound, overflow(val, tp)
	}

	return uint64(val), nil
}

// ConvertUintToUint converts an uint value to another uint value of different precision.
func ConvertUintToUint(val uint64, upperBound uint64, tp byte) (uint64, error) {
	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

// ConvertFloatToUint converts a float value to an uint value.
func ConvertFloatToUint(sc *stmtctx.StatementContext, fval float64, upperBound uint64, tp byte) (uint64, error) {
	val := RoundFloat(fval)
	if val < 0 {
		if sc.ShouldClipToZero() {
			return 0, overflow(val, tp)
		}
		return uint64(int64(val)), overflow(val, tp)
	}

	ubf := float64(upperBound)
	// Because math.MaxUint64 can not be represented precisely in iee754(64bit),
	// so `float64(math.MaxUint64)` will make a num bigger than math.MaxUint64,
	// which can not be represented by 64bit integer.
	// So `uint64(float64(math.MaxUint64))` is undefined behavior.
	if val == ubf {
		return uint64(math.MaxInt64), nil
	}
	if val > ubf {
		return uint64(math.MaxInt64), overflow(val, tp)
	}
	return uint64(val), nil
}

// convertScientificNotation converts a decimal string with scientific notation to a normal decimal string.
// 1E6 => 1000000, .12345E+5 => 12345
func convertScientificNotation(str string) (string, error) {
	// https://golang.org/ref/spec#Floating-point_literals
	eIdx := -1
	point := -1
	for i := 0; i < len(str); i++ {
		if str[i] == '.' {
			point = i
		}
		if str[i] == 'e' || str[i] == 'E' {
			eIdx = i
			if point == -1 {
				point = i
			}
			break
		}
	}
	if eIdx == -1 {
		return str, nil
	}
	exp, err := strconv.ParseInt(str[eIdx+1:], 10, 64)
	if err != nil {
		return "", errors.WithStack(err)
	}

	f := str[:eIdx]
	if exp == 0 {
		return f, nil
	} else if exp > 0 { // move point right
		if point+int(exp) == len(f)-1 { // 123.456 >> 3 = 123456. = 123456
			return f[:point] + f[point+1:], nil
		} else if point+int(exp) < len(f)-1 { // 123.456 >> 2 = 12345.6
			return f[:point] + f[point+1:point+1+int(exp)] + "." + f[point+1+int(exp):], nil
		}
		// 123.456 >> 5 = 12345600
		return f[:point] + f[point+1:] + strings.Repeat("0", point+int(exp)-len(f)+1), nil
	} else { // move point left
		exp = -exp
		if int(exp) < point { // 123.456 << 2 = 1.23456
			return f[:point-int(exp)] + "." + f[point-int(exp):point] + f[point+1:], nil
		}
		// 123.456 << 5 = 0.00123456
		return "0." + strings.Repeat("0", int(exp)-point) + f[:point] + f[point+1:], nil
	}
}

// StrToInt converts a string to an integer at the best-effort.
func StrToInt(sc *stmtctx.StatementContext, str string) (int64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	iVal, err1 := strconv.ParseInt(validPrefix, 10, 64)
	if err1 != nil {
		return iVal, ErrOverflow.GenWithStackByArgs("BIGINT", validPrefix)
	}
	return iVal, errors.Trace(err)
}

// StrToUint converts a string to an unsigned integer at the best-effortt.
func StrToUint(sc *stmtctx.StatementContext, str string) (uint64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	if validPrefix[0] == '+' {
		validPrefix = validPrefix[1:]
	}
	uVal, err1 := strconv.ParseUint(validPrefix, 10, 64)
	if err1 != nil {
		return uVal, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", validPrefix)
	}
	return uVal, errors.Trace(err)
}

// getValidIntPrefix gets prefix of the string which can be successfully parsed as int.
func getValidIntPrefix(sc *stmtctx.StatementContext, str string) (string, error) {
	if !sc.CastStrToIntStrict {
		floatPrefix, err := getValidFloatPrefix(sc, str)
		if err != nil {
			return floatPrefix, errors.Trace(err)
		}
		return floatStrToIntStr(sc, floatPrefix, str)
	}

	validLen := 0

	for i := 0; i < len(str); i++ {
		c := str[i]
		if (c == '+' || c == '-') && i == 0 {
			continue
		}

		if c >= '0' && c <= '9' {
			validLen = i + 1
			continue
		}

		break
	}
	valid := str[:validLen]
	if valid == "" {
		valid = "0"
	}
	if validLen == 0 || validLen != len(str) {
		return valid, errors.Trace(handleTruncateError(sc, ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", str)))
	}
	return valid, nil
}

// roundIntStr is to round a **valid int string** base on the number following dot.
func roundIntStr(numNextDot byte, intStr string) string {
	if numNextDot < '5' {
		return intStr
	}
	retStr := []byte(intStr)
	idx := len(intStr) - 1
	for ; idx >= 1; idx-- {
		if retStr[idx] != '9' {
			retStr[idx]++
			break
		}
		retStr[idx] = '0'
	}
	if idx == 0 {
		if intStr[0] == '9' {
			retStr[0] = '1'
			retStr = append(retStr, '0')
		} else if isDigit(intStr[0]) {
			retStr[0]++
		} else {
			retStr[1] = '1'
			retStr = append(retStr, '0')
		}
	}
	return string(retStr)
}

// floatStrToIntStr converts a valid float string into valid integer string which can be parsed by
// strconv.ParseInt, we can't parse float first then convert it to string because precision will
// be lost. For example, the string value "18446744073709551615" which is the max number of unsigned
// int will cause some precision to lose. intStr[0] may be a positive and negative sign like '+' or '-'.
//
// This func will find serious overflow such as the len of intStr > 20 (without prefix `+/-`)
// however, it will not check whether the intStr overflow BIGINT.
func floatStrToIntStr(sc *stmtctx.StatementContext, validFloat string, oriStr string) (intStr string, _ error) {
	var dotIdx = -1
	var eIdx = -1
	for i := 0; i < len(validFloat); i++ {
		switch validFloat[i] {
		case '.':
			dotIdx = i
		case 'e', 'E':
			eIdx = i
		}
	}
	if eIdx == -1 {
		if dotIdx == -1 {
			return validFloat, nil
		}
		var digits []byte
		if validFloat[0] == '-' || validFloat[0] == '+' {
			dotIdx--
			digits = []byte(validFloat[1:])
		} else {
			digits = []byte(validFloat)
		}
		if dotIdx == 0 {
			intStr = "0"
		} else {
			intStr = string(digits)[:dotIdx]
		}
		if len(digits) > dotIdx+1 {
			intStr = roundIntStr(digits[dotIdx+1], intStr)
		}
		if (len(intStr) > 1 || intStr[0] != '0') && validFloat[0] == '-' {
			intStr = "-" + intStr
		}
		return intStr, nil
	}
	// intCnt and digits contain the prefix `+/-` if validFloat[0] is `+/-`
	var intCnt int
	digits := make([]byte, 0, len(validFloat))
	if dotIdx == -1 {
		digits = append(digits, validFloat[:eIdx]...)
		intCnt = len(digits)
	} else {
		digits = append(digits, validFloat[:dotIdx]...)
		intCnt = len(digits)
		digits = append(digits, validFloat[dotIdx+1:eIdx]...)
	}
	exp, err := strconv.Atoi(validFloat[eIdx+1:])
	if err != nil {
		return validFloat, errors.Trace(err)
	}
	intCnt += exp
	if exp >= 0 && (intCnt > 21 || intCnt < 0) {
		// MaxInt64 has 19 decimal digits.
		// MaxUint64 has 20 decimal digits.
		// And the intCnt may contain the len of `+/-`,
		// so I use 21 here as the early detection.
		sc.AppendWarning(ErrOverflow.GenWithStackByArgs("BIGINT", oriStr))
		return validFloat[:eIdx], nil
	}
	if intCnt <= 0 {
		intStr = "0"
		if intCnt == 0 && len(digits) > 0 && isDigit(digits[0]) {
			intStr = roundIntStr(digits[0], intStr)
		}
		return intStr, nil
	}
	if intCnt == 1 && (digits[0] == '-' || digits[0] == '+') {
		intStr = "0"
		if len(digits) > 1 {
			intStr = roundIntStr(digits[1], intStr)
		}
		if intStr[0] == '1' {
			intStr = string(digits[:1]) + intStr
		}
		return intStr, nil
	}
	if intCnt <= len(digits) {
		intStr = string(digits[:intCnt])
		if intCnt < len(digits) {
			intStr = roundIntStr(digits[intCnt], intStr)
		}
	} else {
		// convert scientific notation decimal number
		extraZeroCount := intCnt - len(digits)
		intStr = string(digits) + strings.Repeat("0", extraZeroCount)
	}
	return intStr, nil
}

// StrToFloat converts a string to a float64 at the best-effort.
func StrToFloat(sc *stmtctx.StatementContext, str string) (float64, error) {
	str = strings.TrimSpace(str)
	validStr, err := getValidFloatPrefix(sc, str)
	f, err1 := strconv.ParseFloat(validStr, 64)
	if err1 != nil {
		if err2, ok := err1.(*strconv.NumError); ok {
			// value will truncate to MAX/MIN if out of range.
			if err2.Err == strconv.ErrRange {
				err1 = sc.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("DOUBLE", str))
				if math.IsInf(f, 1) {
					f = math.MaxFloat64
				} else if math.IsInf(f, -1) {
					f = -math.MaxFloat64
				}
			}
		}
		return f, errors.Trace(err1)
	}
	return f, errors.Trace(err)
}

// getValidFloatPrefix gets prefix of string which can be successfully parsed as float.
func getValidFloatPrefix(sc *stmtctx.StatementContext, s string) (valid string, err error) {
	if (sc.InDeleteStmt || sc.InSelectStmt) && s == "" {
		return "0", nil
	}

	var (
		sawDot   bool
		sawDigit bool
		validLen int
		eIdx     int
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '+' || c == '-' {
			if i != 0 && i != eIdx+1 { // "1e+1" is valid.
				break
			}
		} else if c == '.' {
			if sawDot || eIdx > 0 { // "1.1." or "1e1.1"
				break
			}
			sawDot = true
			if sawDigit { // "123." is valid.
				validLen = i + 1
			}
		} else if c == 'e' || c == 'E' {
			if !sawDigit { // "+.e"
				break
			}
			if eIdx != 0 { // "1e5e"
				break
			}
			eIdx = i
		} else if c < '0' || c > '9' {
			break
		} else {
			sawDigit = true
			validLen = i + 1
		}
	}
	valid = s[:validLen]
	if valid == "" {
		valid = "0"
	}
	if validLen == 0 || validLen != len(s) {
		err = errors.Trace(handleTruncateError(sc, ErrTruncatedWrongVal.GenWithStackByArgs("FLOAT", s)))
	}
	return valid, err
}

// ToString converts an interface to a string.
func ToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}
