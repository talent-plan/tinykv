// Copyright 2013 The ql Authors. All rights reserved.
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

//go:generate go run generator/compare_vec.go
//go:generate go run generator/control_vec.go
//go:generate go run generator/other_vec.go

package expression

import (
	"fmt"
	"github.com/pingcap/log"
	"sort"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type baseBuiltinFunc struct {
	bufAllocator columnBufferAllocator
	args         []Expression
	ctx          sessionctx.Context
	tp           *types.FieldType
	pbCode       tipb.ScalarFuncSig

	childrenVectorizedOnce *sync.Once
	childrenVectorized     bool
}

func (b *baseBuiltinFunc) PbCode() tipb.ScalarFuncSig {
	return b.pbCode
}

// metadata returns the metadata of a function.
// metadata means some functions contain extra inner fields which will not
// contain in `tipb.Expr.children` but must be pushed down to coprocessor
func (b *baseBuiltinFunc) metadata() proto.Message {
	// We will not use a field to store them because of only
	// a few functions contain implicit parameters
	return nil
}

func (b *baseBuiltinFunc) setPbCode(c tipb.ScalarFuncSig) {
	b.pbCode = c
}

func newBaseBuiltinFunc(ctx sessionctx.Context, args []Expression) baseBuiltinFunc {
	if ctx == nil {
		panic("ctx should not be nil")
	}
	return baseBuiltinFunc{
		bufAllocator:           newLocalSliceBuffer(len(args)),
		childrenVectorizedOnce: new(sync.Once),

		args: args,
		ctx:  ctx,
		tp:   types.NewFieldType(mysql.TypeUnspecified),
	}
}

// newBaseBuiltinFuncWithTp creates a built-in function signature with specified types of arguments and the return type of the function.
// argTps indicates the types of the args, retType indicates the return type of the built-in function.
// Every built-in function needs determined argTps and retType when we create it.
func newBaseBuiltinFuncWithTp(ctx sessionctx.Context, args []Expression, retType types.EvalType, argTps ...types.EvalType) (bf baseBuiltinFunc) {
	if len(args) != len(argTps) {
		panic("unexpected length of args and argTps")
	}
	if ctx == nil {
		panic("ctx should not be nil")
	}
	for i := range args {
		if argTps[i] != args[i].GetType().EvalType() {
			log.Warn(fmt.Sprintf("unmatched arg type %v with %v", argTps[i], args[i].GetType().EvalType()))
		}
	}
	var fieldType *types.FieldType
	switch retType {
	case types.ETInt:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETReal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDouble,
			Flen:    mysql.MaxRealWidth,
			Decimal: types.UnspecifiedLength,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETString:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
		}
	}
	if mysql.HasBinaryFlag(fieldType.Flag) {
		fieldType.Charset, fieldType.Collate = charset.CharsetBin, charset.CollationBin
	} else {
		fieldType.Charset, fieldType.Collate = charset.GetDefaultCharsetAndCollate()
	}
	return baseBuiltinFunc{
		bufAllocator:           newLocalSliceBuffer(len(args)),
		childrenVectorizedOnce: new(sync.Once),

		args: args,
		ctx:  ctx,
		tp:   fieldType,
	}
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

func (b *baseBuiltinFunc) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalTime() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDuration() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalJSON() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalInt(row chunk.Row) (int64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalReal(row chunk.Row) (float64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalString(row chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vectorized() bool {
	return false
}

func (b *baseBuiltinFunc) isChildrenVectorized() bool {
	b.childrenVectorizedOnce.Do(func() {
		b.childrenVectorized = true
		for _, arg := range b.args {
			if !arg.Vectorized() {
				b.childrenVectorized = false
				break
			}
		}
	})
	return b.childrenVectorized
}

func (b *baseBuiltinFunc) getRetTp() *types.FieldType {
	switch b.tp.EvalType() {
	case types.ETString:
		if b.tp.Flen >= mysql.MaxBlobWidth {
			b.tp.Tp = mysql.TypeLongBlob
		} else if b.tp.Flen >= 65536 {
			b.tp.Tp = mysql.TypeMediumBlob
		}
		if len(b.tp.Charset) <= 0 {
			b.tp.Charset, b.tp.Collate = charset.GetDefaultCharsetAndCollate()
		}
	}
	return b.tp
}

func (b *baseBuiltinFunc) equal(fun builtinFunc) bool {
	funArgs := fun.getArgs()
	if len(funArgs) != len(b.args) {
		return false
	}
	for i := range b.args {
		if !b.args[i].Equal(b.ctx, funArgs[i]) {
			return false
		}
	}
	return true
}

func (b *baseBuiltinFunc) getCtx() sessionctx.Context {
	return b.ctx
}

func (b *baseBuiltinFunc) cloneFrom(from *baseBuiltinFunc) {
	b.args = make([]Expression, 0, len(b.args))
	for _, arg := range from.args {
		b.args = append(b.args, arg.Clone())
	}
	b.ctx = from.ctx
	b.tp = from.tp
	b.pbCode = from.pbCode
	b.bufAllocator = newLocalSliceBuffer(len(b.args))
	b.childrenVectorizedOnce = new(sync.Once)
}

func (b *baseBuiltinFunc) Clone() builtinFunc {
	panic("you should not call this method.")
}

// vecBuiltinFunc contains all vectorized methods for a builtin function.
type vecBuiltinFunc interface {
	// vectorized returns if this builtin function itself supports vectorized evaluation.
	vectorized() bool

	// isChildrenVectorized returns if its all children support vectorized evaluation.
	isChildrenVectorized() bool

	// vecEvalInt evaluates this builtin function in a vectorized manner.
	vecEvalInt(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalReal evaluates this builtin function in a vectorized manner.
	vecEvalReal(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalString evaluates this builtin function in a vectorized manner.
	vecEvalString(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalDecimal evaluates this builtin function in a vectorized manner.
	vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalTime evaluates this builtin function in a vectorized manner.
	vecEvalTime(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalDuration evaluates this builtin function in a vectorized manner.
	vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalJSON evaluates this builtin function in a vectorized manner.
	vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error
}

// builtinFunc stands for a particular function signature.
type builtinFunc interface {
	vecBuiltinFunc

	// evalInt evaluates int result of builtinFunc by given row.
	evalInt(row chunk.Row) (val int64, isNull bool, err error)
	// evalReal evaluates real representation of builtinFunc by given row.
	evalReal(row chunk.Row) (val float64, isNull bool, err error)
	// evalString evaluates string representation of builtinFunc by given row.
	evalString(row chunk.Row) (val string, isNull bool, err error)
	// getArgs returns the arguments expressions.
	getArgs() []Expression
	// equal check if this function equals to another function.
	equal(builtinFunc) bool
	// getCtx returns this function's context.
	getCtx() sessionctx.Context
	// getRetTp returns the return type of the built-in function.
	getRetTp() *types.FieldType
	// setPbCode sets pbCode for signature.
	setPbCode(tipb.ScalarFuncSig)
	// PbCode returns PbCode of this signature.
	PbCode() tipb.ScalarFuncSig
	// metadata returns the metadata of a function.
	// metadata means some functions contain extra inner fields which will not
	// contain in `tipb.Expr.children` but must be pushed down to coprocessor
	metadata() proto.Message
	// Clone returns a copy of itself.
	Clone() builtinFunc
}

// baseFunctionClass will be contained in every struct that implement functionClass interface.
type baseFunctionClass struct {
	funcName string
	minArgs  int
	maxArgs  int
}

func (b *baseFunctionClass) verifyArgs(args []Expression) error {
	l := len(args)
	if l < b.minArgs || (b.maxArgs != -1 && l > b.maxArgs) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(b.funcName)
	}
	return nil
}

// functionClass is the interface for a function which may contains multiple functions.
type functionClass interface {
	// getFunction gets a function signature by the types and the counts of given arguments.
	getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error)
}

// funcs holds all registered builtin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]functionClass{
	// common functions
	ast.IsNull: &isNullFunctionClass{baseFunctionClass{ast.IsNull, 1, 1}},

	// string functions
	ast.Length:      &lengthFunctionClass{baseFunctionClass{ast.Length, 1, 1}},
	ast.OctetLength: &lengthFunctionClass{baseFunctionClass{ast.OctetLength, 1, 1}},
	ast.Strcmp:      &strcmpFunctionClass{baseFunctionClass{ast.Strcmp, 2, 2}},

	// control functions
	ast.If:     &ifFunctionClass{baseFunctionClass{ast.If, 3, 3}},
	ast.Ifnull: &ifNullFunctionClass{baseFunctionClass{ast.Ifnull, 2, 2}},

	ast.LogicAnd:   &logicAndFunctionClass{baseFunctionClass{ast.LogicAnd, 2, 2}},
	ast.LogicOr:    &logicOrFunctionClass{baseFunctionClass{ast.LogicOr, 2, 2}},
	ast.GE:         &compareFunctionClass{baseFunctionClass{ast.GE, 2, 2}, opcode.GE},
	ast.LE:         &compareFunctionClass{baseFunctionClass{ast.LE, 2, 2}, opcode.LE},
	ast.EQ:         &compareFunctionClass{baseFunctionClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.NE:         &compareFunctionClass{baseFunctionClass{ast.NE, 2, 2}, opcode.NE},
	ast.LT:         &compareFunctionClass{baseFunctionClass{ast.LT, 2, 2}, opcode.LT},
	ast.GT:         &compareFunctionClass{baseFunctionClass{ast.GT, 2, 2}, opcode.GT},
	ast.Plus:       &arithmeticPlusFunctionClass{baseFunctionClass{ast.Plus, 2, 2}},
	ast.Minus:      &arithmeticMinusFunctionClass{baseFunctionClass{ast.Minus, 2, 2}},
	ast.Div:        &arithmeticDivideFunctionClass{baseFunctionClass{ast.Div, 2, 2}},
	ast.Mul:        &arithmeticMultiplyFunctionClass{baseFunctionClass{ast.Mul, 2, 2}},
	ast.UnaryNot:   &unaryNotFunctionClass{baseFunctionClass{ast.UnaryNot, 1, 1}},
	ast.UnaryMinus: &unaryMinusFunctionClass{baseFunctionClass{ast.UnaryMinus, 1, 1}},
	ast.In:         &inFunctionClass{baseFunctionClass{ast.In, 2, -1}},
	ast.RowFunc:    &rowFunctionClass{baseFunctionClass{ast.RowFunc, 2, -1}},
	ast.SetVar:     &setVarFunctionClass{baseFunctionClass{ast.SetVar, 2, 2}},
	ast.GetVar:     &getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}},
}

// IsFunctionSupported check if given function name is a builtin sql function.
func IsFunctionSupported(name string) bool {
	_, ok := funcs[name]
	return ok
}

// GetBuiltinList returns a list of builtin functions
func GetBuiltinList() []string {
	res := make([]string, 0, len(funcs))
	notImplementedFunctions := []string{ast.RowFunc}
	for funcName := range funcs {
		skipFunc := false
		// Skip not implemented functions
		for _, notImplFunc := range notImplementedFunctions {
			if funcName == notImplFunc {
				skipFunc = true
			}
		}
		// Skip literal functions
		// (their names are not readable: 'tidb`.(dateliteral, for example)
		// See: https://github.com/pingcap/tidb/parser/pull/591
		if strings.HasPrefix(funcName, "'tidb`.(") {
			skipFunc = true
		}
		if skipFunc {
			continue
		}
		res = append(res, funcName)
	}
	sort.Strings(res)
	return res
}
