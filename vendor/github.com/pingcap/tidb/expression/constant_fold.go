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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// FoldConstant does constant folding optimization on an expression excluding deferred ones.
func FoldConstant(expr Expression) Expression {
	return foldConstant(expr)
}

func foldConstant(expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return expr
		}

		args := x.GetArgs()
		sc := x.GetCtx().GetSessionVars().StmtCtx
		argIsConst := make([]bool, len(args))
		hasNullArg := false
		allConstArg := true
		for i := 0; i < len(args); i++ {
			switch x := args[i].(type) {
			case *Constant:
				argIsConst[i] = true
				hasNullArg = hasNullArg || x.Value.IsNull()
			default:
				allConstArg = false
			}
		}
		if !allConstArg {
			if !hasNullArg || !sc.InNullRejectCheck {
				return expr
			}
			constArgs := make([]Expression, len(args))
			for i, arg := range args {
				if argIsConst[i] {
					constArgs[i] = arg
				} else {
					constArgs[i] = One
				}
			}
			dummyScalarFunc, err := NewFunctionBase(x.GetCtx(), x.FuncName.L, x.GetType(), constArgs...)
			if err != nil {
				return expr
			}
			value, err := dummyScalarFunc.Eval(chunk.Row{})
			if err != nil {
				return expr
			}
			if value.IsNull() {
				return &Constant{Value: value, RetType: x.RetType}
			}
			if isTrue, err := value.ToBool(sc); err == nil && isTrue == 0 {
				return &Constant{Value: value, RetType: x.RetType}
			}
			return expr
		}
		value, err := x.Eval(chunk.Row{})
		if err != nil {
			logutil.BgLogger().Debug("fold expression to constant", zap.String("expression", x.ExplainInfo()), zap.Error(err))
			return expr
		}
		return &Constant{Value: value, RetType: x.RetType}
	}
	return expr
}
