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

import (
	"fmt"
	"io"

	"github.com/pingcap/tidb/parser/model"
)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
)

// List scalar function names.
const (
	IsNull      = "isnull"
	Length      = "length"
	Strcmp      = "strcmp"
	OctetLength = "octet_length"
	If          = "if"
	Ifnull      = "ifnull"
	LogicAnd    = "and"
	LogicOr     = "or"
	GE          = "ge"
	LE          = "le"
	EQ          = "eq"
	NE          = "ne"
	LT          = "lt"
	GT          = "gt"
	Plus        = "plus"
	Minus       = "minus"
	Div         = "div"
	Mul         = "mul"
	UnaryNot    = "not"
	UnaryMinus  = "unaryminus"
	In          = "in"
	RowFunc     = "row"
	SetVar      = "setvar"
	GetVar      = "getvar"
	Values      = "values"
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	// FnName is the function name.
	FnName model.CIStr
	// Args is the function args.
	Args []ExprNode
}

// Format the ExprNode into a Writer.
func (n *FuncCallExpr) Format(w io.Writer) {
	fmt.Fprintf(w, "%s(", n.FnName.L)
	for i, arg := range n.Args {
		arg.Format(w)
		if i != len(n.Args)-1 {
			fmt.Fprint(w, ", ")
		}
	}
	fmt.Fprint(w, ")")
}

// Accept implements Node interface.
func (n *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

const (
	// AggFuncCount is the name of Count function.
	AggFuncCount = "count"
	// AggFuncSum is the name of Sum function.
	AggFuncSum = "sum"
	// AggFuncAvg is the name of Avg function.
	AggFuncAvg = "avg"
	// AggFuncFirstRow is the name of FirstRowColumn function.
	AggFuncFirstRow = "firstrow"
	// AggFuncMax is the name of max function.
	AggFuncMax = "max"
	// AggFuncMin is the name of min function.
	AggFuncMin = "min"
)

// AggregateFuncExpr represents aggregate function expression.
type AggregateFuncExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
}

// Format the ExprNode into a Writer.
func (n *AggregateFuncExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AggregateFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}
