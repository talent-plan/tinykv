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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

var (
	_ StmtNode = &AdminStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &UseStmt{}

	_ Node = &VariableAssignment{}
)

const (
	// Valid formats for explain statement.
	ExplainFormatROW = "row"
	ExplainFormatDOT = "dot"
)

var (
	// ExplainFormats stores the valid formats for explain statement, used by validator.
	ExplainFormats = []string{
		ExplainFormatROW,
		ExplainFormatDOT,
	}
)

// TypeOpt is used for parsing data type option from SQL.
type TypeOpt struct {
	IsUnsigned bool
	IsZerofill bool
}

// FloatOpt is used for parsing floating-point type option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html
type FloatOpt struct {
	Flen    int
	Decimal int
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt   StmtNode
	Format string
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(DMLNode)
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// CommitStmt is a statement to commit the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CommitStmt)
	return v.Leave(n)
}

// RollbackStmt is a statement to roll back the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RollbackStmt)
	return v.Leave(n)
}

// UseStmt is a statement to use the DBName database as the current database.
// See https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Accept implements Node Accept interface.
func (n *UseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UseStmt)
	return v.Leave(n)
}

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	node
	Name     string
	Value    ExprNode
	IsGlobal bool
	IsSystem bool
}

// Accept implements Node interface.
func (n *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableAssignment)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node Accept interface.
func (n *SetStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetStmt)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

// AdminStmtType is the type for admin statement.
type AdminStmtType int

// Admin statement types.
const (
	AdminShowDDL = iota + 1
	AdminShowDDLJobs
)

// AdminStmt is the struct for Admin statement.
type AdminStmt struct {
	stmtNode

	Tp        AdminStmtType
	Tables    []*TableName
	JobNumber int64
	Where     ExprNode
}

// Accept implements Node Accept interface.
func (n *AdminStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*AdminStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}

	return v.Leave(n)
}

// Ident is the table identifier composed of schema name and table name.
type Ident struct {
	Schema model.CIStr
	Name   model.CIStr
}

// String implements fmt.Stringer interface.
func (i Ident) String() string {
	if i.Schema.O == "" {
		return i.Name.O
	}
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}

// SelectStmtOpts wrap around select hints and switches
type SelectStmtOpts struct {
	Distinct        bool
	SQLBigResult    bool
	SQLBufferResult bool
	SQLCache        bool
	SQLSmallResult  bool
	CalcFoundRows   bool
	StraightJoin    bool
	Priority        mysql.PriorityEnum
	TableHints      []*TableOptimizerHint
}

// TableOptimizerHint is Table level optimizer hint
type TableOptimizerHint struct {
	node
	// HintName is the name or alias of the table(s) which the hint will affect.
	// Table hints has no schema info
	// It allows only table name or alias (if table has an alias)
	HintName model.CIStr
	// QBName is the default effective query block of this hint.
	QBName    model.CIStr
	Tables    []HintTable
	Indexes   []model.CIStr
	StoreType model.CIStr
	// Statement Execution Time Optimizer Hints
	// See https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html#optimizer-hints-execution-time
	MaxExecutionTime uint64
	MemoryQuota      int64
	QueryType        model.CIStr
	HintFlag         bool
}

// HintTable is table in the hint. It may have query block info.
type HintTable struct {
	DBName    model.CIStr
	TableName model.CIStr
	QBName    model.CIStr
}

// Accept implements Node Accept interface.
func (n *TableOptimizerHint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableOptimizerHint)
	return v.Leave(n)
}

type BinaryLiteral interface {
	ToString() string
}

// NewDecimal creates a types.Decimal value, it's provided by parser driver.
var NewDecimal func(string) (interface{}, error)

// NewHexLiteral creates a types.HexLiteral value, it's provided by parser driver.
var NewHexLiteral func(string) (interface{}, error)

// NewBitLiteral creates a types.BitLiteral value, it's provided by parser driver.
var NewBitLiteral func(string) (interface{}, error)
