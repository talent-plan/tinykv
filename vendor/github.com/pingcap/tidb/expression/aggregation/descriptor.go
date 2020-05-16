// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (*AggFuncDesc, error) {
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &AggFuncDesc{baseFuncDesc: b}, nil
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx sessionctx.Context, other *AggFuncDesc) bool {
	return a.baseFuncDesc.equal(ctx, &other.baseFuncDesc)
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	clone.baseFuncDesc = *a.baseFuncDesc.clone()
	return &clone
}

// Split splits `a` into two aggregate descriptors for partial phase and
// final phase individually.
// This function is only used when executing aggregate function parallelly.
// ordinal indicates the column ordinal of the intermediate result.
func (a *AggFuncDesc) Split(ordinal []int) (partialAggDesc, finalAggDesc *AggFuncDesc) {
	partialAggDesc = a.Clone()
	if a.Mode == CompleteMode {
		partialAggDesc.Mode = Partial1Mode
	} else if a.Mode == FinalMode {
		partialAggDesc.Mode = Partial2Mode
	} else {
		panic("Error happened during AggFuncDesc.Split, the AggFunctionMode is not CompleteMode or FinalMode.")
	}
	finalAggDesc = &AggFuncDesc{
		Mode: FinalMode, // We only support FinalMode now in final phase.
	}
	finalAggDesc.Name = a.Name
	finalAggDesc.RetTp = a.RetTp
	switch a.Name {
	case ast.AggFuncAvg:
		args := make([]expression.Expression, 0, 2)
		args = append(args, &expression.Column{
			Index:   ordinal[0],
			RetType: types.NewFieldType(mysql.TypeLonglong),
		})
		args = append(args, &expression.Column{
			Index:   ordinal[1],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	default:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.Column{
			Index:   ordinal[0],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	}
	return
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching row for the inner table of an outer join,
// an aggregation function only involves constant and/or columns belongs to the inner table
// will be set to the null value.
// The input stands for the schema of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Table t with only one row:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Table s which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.evalNullValueInOuterJoin4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstRow:
		return a.evalNullValueInOuterJoin4Sum(ctx, schema)
	case ast.AggFuncAvg:
		return types.Datum{}, false
	default:
		panic("unsupported agg function")
	}
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx sessionctx.Context) Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: aggFunc, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: aggFunc, isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: aggFunc}
	default:
		panic("unsupported agg function")
	}
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	for _, arg := range a.Args {
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			return types.Datum{}, ok
		}
	}
	return types.NewDatum(1), true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.Datum{}, ok
	}
	return con.Value, true
}
