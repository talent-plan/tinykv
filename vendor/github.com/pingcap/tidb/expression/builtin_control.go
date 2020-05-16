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

package expression

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfStringSig{}
)

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, LEAD and LAG.
func InferType4ControlFuncs(lhs, rhs *types.FieldType) *types.FieldType {
	resultFieldType := &types.FieldType{}
	if lhs.Tp == mysql.TypeNull {
		*resultFieldType = *rhs
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.Tp == mysql.TypeNull {
			resultFieldType.Tp = mysql.TypeString
			resultFieldType.Flen, resultFieldType.Decimal = 0, 0
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else if rhs.Tp == mysql.TypeNull {
		*resultFieldType = *lhs
	} else {
		resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
		evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &resultFieldType.Flag)
		if evalType == types.ETInt {
			resultFieldType.Decimal = 0
		} else {
			if lhs.Decimal == types.UnspecifiedLength || rhs.Decimal == types.UnspecifiedLength {
				resultFieldType.Decimal = types.UnspecifiedLength
			} else {
				resultFieldType.Decimal = mathutil.Max(lhs.Decimal, rhs.Decimal)
			}
		}
		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
			if mysql.HasBinaryFlag(lhs.Flag) || !types.IsNonBinaryStr(rhs) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
			if mysql.HasBinaryFlag(rhs.Flag) || !types.IsNonBinaryStr(lhs) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringKind() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		}
		if evalType == types.ETInt {
			lhsUnsignedFlag, rhsUnsignedFlag := mysql.HasUnsignedFlag(lhs.Flag), mysql.HasUnsignedFlag(rhs.Flag)
			lhsFlagLen, rhsFlagLen := 0, 0
			if !lhsUnsignedFlag {
				lhsFlagLen = 1
			}
			if !rhsUnsignedFlag {
				rhsFlagLen = 1
			}
			lhsFlen := lhs.Flen - lhsFlagLen
			rhsFlen := rhs.Flen - rhsFlagLen
			if lhs.Decimal != types.UnspecifiedLength {
				lhsFlen -= lhs.Decimal
			}
			if lhs.Decimal != types.UnspecifiedLength {
				rhsFlen -= rhs.Decimal
			}
			resultFieldType.Flen = mathutil.Max(lhsFlen, rhsFlen) + resultFieldType.Decimal + 1
		} else {
			resultFieldType.Flen = mathutil.Max(lhs.Flen, rhs.Flen)
		}
	}
	// Fix decimal for int and string.
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.ETInt {
		resultFieldType.Decimal = 0
	} else if resultEvalType == types.ETString {
		if lhs.Tp != mysql.TypeNull || rhs.Tp != mysql.TypeNull {
			resultFieldType.Decimal = types.UnspecifiedLength
		}
	}
	return resultFieldType
}

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp := InferType4ControlFuncs(args[1].GetType(), args[2].GetType())
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, types.ETInt, evalTps, evalTps)
	retTp.Flag |= bf.tp.Flag
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfInt)
	case types.ETReal:
		sig = &builtinIfRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfReal)
	case types.ETString:
		sig = &builtinIfStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfString)
	}
	return sig, nil
}

type builtinIfIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfIntSig) Clone() builtinFunc {
	newSig := &builtinIfIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.args[2].EvalInt(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalReal(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.args[2].EvalReal(b.ctx, row)
	return arg2, isNull2, err
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(row chunk.Row) (ret string, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return "", true, err
	}
	arg1, isNull1, err := b.args[1].EvalString(b.ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.args[2].EvalString(b.ctx, row)
	return arg2, isNull2, err
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp := InferType4ControlFuncs(lhs, rhs)
	retTp.Flag |= (lhs.Flag & mysql.NotNullFlag) | (rhs.Flag & mysql.NotNullFlag)
	if lhs.Tp == mysql.TypeNull && rhs.Tp == mysql.TypeNull {
		retTp.Tp = mysql.TypeNull
		retTp.Flen, retTp.Decimal = 0, -1
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, evalTps, evalTps)
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfNullIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		sig = &builtinIfNullRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullReal)
	case types.ETString:
		sig = &builtinIfNullStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullString)
	}
	return sig, nil
}

type builtinIfNullIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullIntSig) Clone() builtinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullRealSig) Clone() builtinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalReal(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullStringSig) Clone() builtinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalString(b.ctx, row)
	return arg1, isNull || err != nil, err
}
