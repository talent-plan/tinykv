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
	"strings"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
)

var (
	_ ExprNode = &BetweenExpr{}
	_ ExprNode = &BinaryOperationExpr{}
	_ ExprNode = &ColumnNameExpr{}
	_ ExprNode = &DefaultExpr{}
	_ ExprNode = &IsNullExpr{}
	_ ExprNode = &ParenthesesExpr{}
	_ ExprNode = &PatternInExpr{}
	_ ExprNode = &RowExpr{}
	_ ExprNode = &UnaryOperationExpr{}
	_ ExprNode = &ValuesExpr{}
	_ ExprNode = &VariableExpr{}

	_ Node = &ColumnName{}
)

// ValueExpr define a interface for ValueExpr.
type ValueExpr interface {
	ExprNode
	SetValue(val interface{})
	GetValue() interface{}
	GetDatumString() string
	GetString() string
	GetProjectionOffset() int
	SetProjectionOffset(offset int)
}

// NewValueExpr creates a ValueExpr with value, and sets default field type.
var NewValueExpr func(interface{}) ValueExpr

// BetweenExpr is for "between and" or "not between and" expression.
type BetweenExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Left is the expression for minimal value in the range.
	Left ExprNode
	// Right is the expression for maximum value in the range.
	Right ExprNode
	// Not is true, the expression is "not between and".
	Not bool
}

// Format the ExprNode into a Writer.
func (n *BetweenExpr) Format(w io.Writer) {
	n.Expr.Format(w)
	if n.Not {
		fmt.Fprint(w, " NOT BETWEEN ")
	} else {
		fmt.Fprint(w, " BETWEEN ")
	}
	n.Left.Format(w)
	fmt.Fprint(w, " AND ")
	n.Right.Format(w)
}

// Accept implements Node interface.
func (n *BetweenExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BetweenExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)

	node, ok = n.Left.Accept(v)
	if !ok {
		return n, false
	}
	n.Left = node.(ExprNode)

	node, ok = n.Right.Accept(v)
	if !ok {
		return n, false
	}
	n.Right = node.(ExprNode)

	return v.Leave(n)
}

// BinaryOperationExpr is for binary operation like `1 + 1`, `1 - 1`, etc.
type BinaryOperationExpr struct {
	exprNode
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L ExprNode
	// R is the right expression in BinaryOperation.
	R ExprNode
}

// Format the ExprNode into a Writer.
func (n *BinaryOperationExpr) Format(w io.Writer) {
	n.L.Format(w)
	fmt.Fprint(w, " ")
	n.Op.Format(w)
	fmt.Fprint(w, " ")
	n.R.Format(w)
}

// Accept implements Node interface.
func (n *BinaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BinaryOperationExpr)
	node, ok := n.L.Accept(v)
	if !ok {
		return n, false
	}
	n.L = node.(ExprNode)

	node, ok = n.R.Accept(v)
	if !ok {
		return n, false
	}
	n.R = node.(ExprNode)

	return v.Leave(n)
}

// ColumnName represents column name.
type ColumnName struct {
	node
	Schema model.CIStr
	Table  model.CIStr
	Name   model.CIStr
}

// Accept implements Node Accept interface.
func (n *ColumnName) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnName)
	return v.Leave(n)
}

// String implements Stringer interface.
func (n *ColumnName) String() string {
	result := n.Name.L
	if n.Table.L != "" {
		result = n.Table.L + "." + result
	}
	if n.Schema.L != "" {
		result = n.Schema.L + "." + result
	}
	return result
}

// OrigColName returns the full original column name.
func (n *ColumnName) OrigColName() (ret string) {
	ret = n.Name.O
	if n.Table.O == "" {
		return
	}
	ret = n.Table.O + "." + ret
	if n.Schema.O == "" {
		return
	}
	ret = n.Schema.O + "." + ret
	return
}

// ColumnNameExpr represents a column name expression.
type ColumnNameExpr struct {
	exprNode

	// Name is the referenced column name.
	Name *ColumnName

	// Refer is the result field the column name refers to.
	// The value of Refer.Expr is used as the value of the expression.
	Refer *ResultField
}

// Format the ExprNode into a Writer.
func (n *ColumnNameExpr) Format(w io.Writer) {
	name := strings.Replace(n.Name.String(), ".", "`.`", -1)
	fmt.Fprintf(w, "`%s`", name)
}

// Accept implements Node Accept interface.
func (n *ColumnNameExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnNameExpr)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*ColumnName)
	return v.Leave(n)
}

// DefaultExpr is the default expression using default value for a column.
type DefaultExpr struct {
	exprNode
	// Name is the column name.
	Name *ColumnName
}

// Format the ExprNode into a Writer.
func (n *DefaultExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *DefaultExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DefaultExpr)
	if n.Name != nil {
		node, ok := n.Name.Accept(v)
		if !ok {
			return n, false
		}
		n.Name = node.(*ColumnName)
	}
	return v.Leave(n)
}

// PatternInExpr is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternInExpr struct {
	exprNode
	// Expr is the value expression to be compared.
	Expr ExprNode
	// List is the list expression in compare list.
	List []ExprNode
	// Not is true, the expression is "not in".
	Not bool
}

// Format the ExprNode into a Writer.
func (n *PatternInExpr) Format(w io.Writer) {
	n.Expr.Format(w)
	if n.Not {
		fmt.Fprint(w, " NOT IN (")
	} else {
		fmt.Fprint(w, " IN (")
	}
	for i, expr := range n.List {
		if i != 0 {
			fmt.Fprint(w, ",")
		}
		expr.Format(w)
	}
	fmt.Fprint(w, ")")
}

// Accept implements Node Accept interface.
func (n *PatternInExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PatternInExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	for i, val := range n.List {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.List[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// IsNullExpr is the expression for null check.
type IsNullExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not null".
	Not bool
}

// Format the ExprNode into a Writer.
func (n *IsNullExpr) Format(w io.Writer) {
	n.Expr.Format(w)
	if n.Not {
		fmt.Fprint(w, " IS NOT NULL")
		return
	}
	fmt.Fprint(w, " IS NULL")
}

// Accept implements Node Accept interface.
func (n *IsNullExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IsNullExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// ParenthesesExpr is the parentheses expression.
type ParenthesesExpr struct {
	exprNode
	// Expr is the expression in parentheses.
	Expr ExprNode
}

// Format the ExprNode into a Writer.
func (n *ParenthesesExpr) Format(w io.Writer) {
	fmt.Fprint(w, "(")
	n.Expr.Format(w)
	fmt.Fprint(w, ")")
}

// Accept implements Node Accept interface.
func (n *ParenthesesExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ParenthesesExpr)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

// RowExpr is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type RowExpr struct {
	exprNode

	Values []ExprNode
}

// Format the ExprNode into a Writer.
func (n *RowExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *RowExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RowExpr)
	for i, val := range n.Values {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Values[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// UnaryOperationExpr is the expression for unary operator.
type UnaryOperationExpr struct {
	exprNode
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V ExprNode
}

// Format the ExprNode into a Writer.
func (n *UnaryOperationExpr) Format(w io.Writer) {
	n.Op.Format(w)
	n.V.Format(w)
}

// Accept implements Node Accept interface.
func (n *UnaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UnaryOperationExpr)
	node, ok := n.V.Accept(v)
	if !ok {
		return n, false
	}
	n.V = node.(ExprNode)
	return v.Leave(n)
}

// ValuesExpr is the expression used in INSERT VALUES.
type ValuesExpr struct {
	exprNode
	// Column is column name.
	Column *ColumnNameExpr
}

// Format the ExprNode into a Writer.
func (n *ValuesExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ValuesExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValuesExpr)
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	// `node` may be *ast.ValueExpr, to avoid panic, we write `ok` but do not use
	// it.
	n.Column, _ = node.(*ColumnNameExpr)
	return v.Leave(n)
}

// VariableExpr is the expression for variable.
type VariableExpr struct {
	exprNode
	// Name is the variable name.
	Name string
	// IsGlobal indicates whether this variable is global.
	IsGlobal bool
	// IsSystem indicates whether this variable is a system variable in current session.
	IsSystem bool
	// ExplicitScope indicates whether this variable scope is set explicitly.
	ExplicitScope bool
	// Value is the variable value.
	Value ExprNode
}

// Format the ExprNode into a Writer.
func (n *VariableExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *VariableExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableExpr)
	if n.Value == nil {
		return v.Leave(n)
	}

	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}
