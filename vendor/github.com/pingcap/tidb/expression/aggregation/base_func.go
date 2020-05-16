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
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// baseFuncDesc describes an function signature, only used in planner.
type baseFuncDesc struct {
	// Name represents the function name.
	Name string
	// Args represents the arguments of the function.
	Args []expression.Expression
	// RetTp represents the return type of the function.
	RetTp *types.FieldType
}

func newBaseFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (baseFuncDesc, error) {
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	err := b.typeInfer(ctx)
	if err != nil {
		return b, err
	}
	if _, ok := noNeedCastAggFuncs[name]; ok {
		return b, nil
	}
	for _, arg := range args {
		if arg.GetType().EvalType() != b.RetTp.EvalType() {
			log.Warn(fmt.Sprintf("unmatched arg tp %v with return tp %v", arg.GetType().EvalType(), b.RetTp.EvalType()))
		}
	}
	return b, nil
}

func (a *baseFuncDesc) equal(ctx sessionctx.Context, other *baseFuncDesc) bool {
	if a.Name != other.Name || len(a.Args) != len(other.Args) {
		return false
	}
	for i := range a.Args {
		if !a.Args[i].Equal(ctx, other.Args[i]) {
			return false
		}
	}
	return true
}

func (a *baseFuncDesc) clone() *baseFuncDesc {
	clone := *a
	newTp := *a.RetTp
	clone.RetTp = &newTp
	clone.Args = make([]expression.Expression, len(a.Args))
	for i := range a.Args {
		clone.Args[i] = a.Args[i].Clone()
	}
	return &clone
}

// String implements the fmt.Stringer interface.
func (a *baseFuncDesc) String() string {
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	for i, arg := range a.Args {
		buffer.WriteString(arg.String())
		if i+1 != len(a.Args) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// typeInfer infers the arguments and return types of an function.
func (a *baseFuncDesc) typeInfer(ctx sessionctx.Context) error {
	switch a.Name {
	case ast.AggFuncCount:
		a.typeInfer4Count(ctx)
	case ast.AggFuncSum:
		a.typeInfer4Sum(ctx)
	case ast.AggFuncAvg:
		a.typeInfer4Avg(ctx)
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		a.typeInfer4MaxMin(ctx)
	default:
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	return nil
}

func (a *baseFuncDesc) typeInfer4Count(ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Sum should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Sum(ctx sessionctx.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Avg should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Avg(ctx sessionctx.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4MaxMin(ctx sessionctx.Context) {
	a.RetTp = a.Args[0].GetType()
	if (a.Name == ast.AggFuncMax || a.Name == ast.AggFuncMin) && a.RetTp.Tp != mysql.TypeBit {
		a.RetTp = a.Args[0].GetType().Clone()
		a.RetTp.Flag &^= mysql.NotNullFlag
	}
	if a.RetTp.Tp == mysql.TypeEnum || a.RetTp.Tp == mysql.TypeSet {
		a.RetTp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxFieldCharLength}
	}
}

// GetDefaultValue gets the default value when the function's input is null.
// According to MySQL, default values of the function are listed as follows:
// e.g.
// Table t which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select a, avg(a), sum(a), count(a), bit_xor(a), bit_or(a), bit_and(a), max(a), min(a), group_concat(a) from t;`
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | a    | avg(a) | sum(a) | count(a) | bit_xor(a) | bit_or(a) | bit_and(a)           | max(a) | min(a) | group_concat(a) |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | NULL |   NULL |   NULL |        0 |          0 |         0 | 18446744073709551615 |   NULL |   NULL | NULL            |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
func (a *baseFuncDesc) GetDefaultValue() (v types.Datum) {
	switch a.Name {
	case ast.AggFuncCount:
		v = types.NewIntDatum(0)
	case ast.AggFuncFirstRow, ast.AggFuncAvg, ast.AggFuncSum, ast.AggFuncMax,
		ast.AggFuncMin:
		v = types.Datum{}
	}
	return
}

// We do not need to wrap cast upon these functions,
// since the EvalXXX method called by the arg is determined by the corresponding arg type.
var noNeedCastAggFuncs = map[string]struct{}{
	ast.AggFuncCount:    {},
	ast.AggFuncMax:      {},
	ast.AggFuncMin:      {},
	ast.AggFuncFirstRow: {},
}
