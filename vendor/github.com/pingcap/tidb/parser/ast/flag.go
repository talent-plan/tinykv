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

package ast

// HasAggFlag checks if the expr contains FlagHasAggregateFunc.
func HasAggFlag(expr ExprNode) bool {
	return expr.GetFlag()&FlagHasAggregateFunc > 0
}

// SetFlag sets flag for expression.
func SetFlag(n Node) {
	var setter flagSetter
	n.Accept(&setter)
}

type flagSetter struct {
}

func (f *flagSetter) Enter(in Node) (Node, bool) {
	return in, false
}

func (f *flagSetter) Leave(in Node) (Node, bool) {
	switch x := in.(type) {
	case *AggregateFuncExpr:
		f.aggregateFunc(x)
	case *BetweenExpr:
		x.SetFlag(x.Expr.GetFlag() | x.Left.GetFlag() | x.Right.GetFlag())
	case *BinaryOperationExpr:
		x.SetFlag(x.L.GetFlag() | x.R.GetFlag())
	case *ColumnNameExpr:
		x.SetFlag(FlagHasReference)
	case *DefaultExpr:
		x.SetFlag(FlagHasDefault)
	case *FuncCallExpr:
		f.funcCall(x)
	case *IsNullExpr:
		x.SetFlag(x.Expr.GetFlag())
	case *ParenthesesExpr:
		x.SetFlag(x.Expr.GetFlag())
	case *PatternInExpr:
		f.patternIn(x)
	case *RowExpr:
		f.row(x)
	case *UnaryOperationExpr:
		x.SetFlag(x.V.GetFlag())
	case *ValuesExpr:
		x.SetFlag(FlagHasReference)
	case *VariableExpr:
		if x.Value == nil {
			x.SetFlag(FlagHasVariable)
		} else {
			x.SetFlag(FlagHasVariable | x.Value.GetFlag())
		}
	}

	return in, true
}

func (f *flagSetter) patternIn(x *PatternInExpr) {
	flag := x.Expr.GetFlag()
	for _, val := range x.List {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) row(x *RowExpr) {
	var flag uint64
	for _, val := range x.Values {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) funcCall(x *FuncCallExpr) {
	flag := FlagHasFunc
	for _, val := range x.Args {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}

func (f *flagSetter) aggregateFunc(x *AggregateFuncExpr) {
	flag := FlagHasAggregateFunc
	for _, val := range x.Args {
		flag |= val.GetFlag()
	}
	x.SetFlag(flag)
}
