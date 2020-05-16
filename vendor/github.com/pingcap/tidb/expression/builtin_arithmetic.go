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

package expression

import (
	"fmt"
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticMinusRealSig{}
	_ builtinFunc = &builtinArithmeticMinusIntSig{}
	_ builtinFunc = &builtinArithmeticDivideRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntSig{}
)

// numericContextResultType returns types.EvalType for numeric function's parameters.
// the returned types.EvalType should be one of: types.ETInt, types.ETDecimal, types.ETReal
func numericContextResultType(ft *types.FieldType) types.EvalType {
	if types.IsBinaryStr(ft) {
		return types.ETInt
	}
	evalTp4Ft := types.ETReal
	if !ft.Hybrid() {
		evalTp4Ft = ft.EvalType()
		if evalTp4Ft != types.ETInt {
			evalTp4Ft = types.ETReal
		}
	}
	return evalTp4Ft
}

// setFlenDecimal4Int is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4Int(retTp, a, b *types.FieldType) {
	retTp.Decimal = 0
	retTp.Flen = mysql.MaxIntWidth
}

// setFlenDecimal4RealOrDecimal is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4RealOrDecimal(retTp, a, b *types.FieldType, isReal bool, isMultiply bool) {
	if a.Decimal != types.UnspecifiedLength && b.Decimal != types.UnspecifiedLength {
		retTp.Decimal = a.Decimal + b.Decimal
		if !isMultiply {
			retTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
		}
		if !isReal && retTp.Decimal > mysql.MaxDecimalScale {
			retTp.Decimal = mysql.MaxDecimalScale
		}
		if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
			retTp.Flen = types.UnspecifiedLength
			return
		}
		digitsInt := mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal)
		if isMultiply {
			digitsInt = a.Flen - a.Decimal + b.Flen - b.Decimal
		}
		retTp.Flen = digitsInt + retTp.Decimal + 3
		if isReal {
			retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxRealWidth)
			return
		}
		retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxDecimalWidth)
		return
	}
	if isReal {
		retTp.Flen, retTp.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
	} else {
		retTp.Flen, retTp.Decimal = mysql.MaxDecimalWidth, mysql.MaxDecimalScale
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivReal(retTp *types.FieldType) {
	retTp.Decimal = types.UnspecifiedLength
	retTp.Flen = mysql.MaxRealWidth
}

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true, false)
		sig := &builtinArithmeticPlusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_PlusReal)
		return sig, nil
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	if mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag) {
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
	sig := &builtinArithmeticPlusIntSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_PlusInt)
	return sig, nil
}

type builtinArithmeticPlusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b < 0 && uint64(-b) > uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if b > 0 && uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 && uint64(-a) > uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if a > 0 && uint64(b) > math.MaxUint64-uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	return a + b, false, nil
}

type builtinArithmeticPlusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (a > 0 && b > math.MaxFloat64-a) || (a < 0 && b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	return a + b, false, nil
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true, false)
		sig := &builtinArithmeticMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MinusReal)
		return sig, nil
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
	if (mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag)) && !ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode() {
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	sig := &builtinArithmeticMinusIntSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_MinusInt)
	return sig, nil
}

type builtinArithmeticMinusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (a > 0 && -b > math.MaxFloat64-a) || (a < 0 && -b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	return a - b, false, nil
}

type builtinArithmeticMinusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	forceToSigned := s.ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode()
	isLHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	if forceToSigned && mysql.HasUnsignedFlag(s.args[0].GetType().Flag) {
		if a < 0 || (a > math.MaxInt64) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}
	if forceToSigned && mysql.HasUnsignedFlag(s.args[1].GetType().Flag) {
		if b < 0 || (b > math.MaxInt64) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b >= 0 && uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
		if b < 0 && uint64(a) > math.MaxUint64-uint64(-b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 || uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		// We need `(a >= 0 && b == math.MinInt64)` due to `-(math.MinInt64) == math.MinInt64`.
		// If `a<0 && b<=0`: `a-b` will not overflow even though b==math.MinInt64.
		// If `a<0 && b>0`: `a-b` will not overflow only if `math.MinInt64<=a-b` satisfied
		if (a >= 0 && b == math.MinInt64) || (a > 0 && -b > math.MaxInt64-a) || (a < 0 && -b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	}
	return a - b, false, nil
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true, true)
		sig := &builtinArithmeticMultiplyRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig, nil
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
		bf.tp.Flag |= mysql.UnsignedFlag
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticMultiplyIntUnsignedSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyIntUnsigned)
		return sig, nil
	}
	setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
	sig := &builtinArithmeticMultiplyIntSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
	return sig, nil
}

type builtinArithmeticMultiplyRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntUnsignedSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if a != 0 && result/a != b {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
	c.setType4DivReal(bf.tp)
	sig := &builtinArithmeticDivideRealSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DivideReal)
	return sig, nil
}

type builtinArithmeticDivideRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticDivideRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	result := a / b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}
