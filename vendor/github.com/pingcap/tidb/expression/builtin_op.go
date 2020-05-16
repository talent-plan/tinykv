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

package expression

import (
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &logicAndFunctionClass{}
	_ functionClass = &logicOrFunctionClass{}
	_ functionClass = &unaryMinusFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
	_ functionClass = &unaryNotFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
	_ builtinFunc = &builtinLogicOrSig{}
	_ builtinFunc = &builtinUnaryMinusIntSig{}
	_ builtinFunc = &builtinIntIsNullSig{}
	_ builtinFunc = &builtinRealIsNullSig{}
	_ builtinFunc = &builtinStringIsNullSig{}
	_ builtinFunc = &builtinUnaryNotRealSig{}
	_ builtinFunc = &builtinUnaryNotIntSig{}
)

type logicAndFunctionClass struct {
	baseFunctionClass
}

func (c *logicAndFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.tp.Flen = 1
	return sig, nil
}

type builtinLogicAndSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicAndSig) Clone() builtinFunc {
	newSig := &builtinLogicAndSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicAndSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil || (!isNull0 && arg0 == 0) {
		return 0, err != nil, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil || (!isNull1 && arg1 == 0) {
		return 0, err != nil, err
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 1, false, nil
}

type logicOrFunctionClass struct {
	baseFunctionClass
}

func (c *logicOrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 1
	sig := &builtinLogicOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig, nil
}

type builtinLogicOrSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicOrSig) Clone() builtinFunc {
	newSig := &builtinLogicOrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicOrSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return 1, false, nil
	}
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull1 && arg1 != 0 {
		return 1, false, nil
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 0, false, nil
}

type unaryNotFunctionClass struct {
	baseFunctionClass
}

func (c *unaryNotFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	if argTp == types.ETString {
		argTp = types.ETReal
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1

	var sig builtinFunc
	switch argTp {
	case types.ETReal:
		sig = &builtinUnaryNotRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotReal)
	case types.ETInt:
		sig = &builtinUnaryNotIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryNotInt)
	default:
		return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
	}
	return sig, nil
}

type builtinUnaryNotRealSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryNotRealSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryNotIntSig) Clone() builtinFunc {
	newSig := &builtinUnaryNotIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type unaryMinusFunctionClass struct {
	baseFunctionClass
}

// typeInfer infers unaryMinus function return type. when the arg is an int constant and overflow,
// typerInfer will infers the return type as types.ETDecimal, not types.ETInt.
func (c *unaryMinusFunctionClass) typeInfer(argExpr Expression) (types.EvalType, bool) {
	tp := argExpr.GetType().EvalType()
	if tp != types.ETInt {
		tp = types.ETReal
	}

	return tp, false
}

func (c *unaryMinusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	argExpr, argExprTp := args[0], args[0].GetType()
	_, intOverflow := c.typeInfer(argExpr)

	var bf baseBuiltinFunc
	switch argExprTp.EvalType() {
	case types.ETInt:
		if intOverflow {
			panic("overflows int")
		} else {
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
			sig = &builtinUnaryMinusIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusInt)
		}
		bf.tp.Decimal = 0
	case types.ETReal:
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
		sig = &builtinUnaryMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	default:
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
		sig = &builtinUnaryMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	}
	bf.tp.Flen = argExprTp.Flen + 1
	return sig, err
}

type builtinUnaryMinusIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusIntSig) Clone() builtinFunc {
	newSig := &builtinUnaryMinusIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	var val int64
	val, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if err != nil || isNull {
		return val, isNull, err
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		uval := uint64(val)
		if uval > uint64(-math.MinInt64) {
			return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", uval))
		} else if uval == uint64(-math.MinInt64) {
			return math.MinInt64, false, nil
		}
	} else if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", val))
	}
	return -val, false, nil
}

type builtinUnaryMinusRealSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) Clone() builtinFunc {
	newSig := &builtinUnaryMinusRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, row)
	return -val, isNull, err
}

type isNullFunctionClass struct {
	baseFunctionClass
}

func (c *isNullFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType().EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		sig = &builtinIntIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntIsNull)
	case types.ETReal:
		sig = &builtinRealIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_RealIsNull)
	case types.ETString:
		sig = &builtinStringIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StringIsNull)
	default:
		panic("unexpected types.EvalType")
	}
	return sig, nil
}

func evalIsNull(isNull bool, err error) (int64, bool, error) {
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinIntIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsNullSig) Clone() builtinFunc {
	newSig := &builtinIntIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalInt(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinRealIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsNullSig) Clone() builtinFunc {
	newSig := &builtinRealIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalReal(b.ctx, row)
	return evalIsNull(isNull, err)
}

type builtinStringIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinStringIsNullSig) Clone() builtinFunc {
	newSig := &builtinStringIsNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStringIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalString(b.ctx, row)
	return evalIsNull(isNull, err)
}
