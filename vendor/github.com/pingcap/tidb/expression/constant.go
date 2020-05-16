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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

var (
	// One stands for a number 1.
	One = &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Zero stands for a number 0.
	Zero = &Constant{
		Value:   types.NewDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Null stands for null constant.
	Null = &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
)

// Constant stands for a constant value.
type Constant struct {
	Value    types.Datum
	RetType  *types.FieldType
	hashcode []byte
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", c)), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	return c
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.RetType
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, c, types.ETInt, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, c, types.ETReal, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	return genVecFromConstExpr(ctx, c, types.ETString, input, result)
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ chunk.Row) (types.Datum, error) {
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx sessionctx.Context, _ chunk.Row) (int64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx sessionctx.Context, _ chunk.Row) (float64, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || c.Value.Kind() == types.KindString {
		res, err := c.Value.ToFloat64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	return c.Value.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sessionctx.Context, _ chunk.Row) (string, bool, error) {
	if c.GetType().Tp == mysql.TypeNull || c.Value.IsNull() {
		return "", true, nil
	}
	res, err := c.Value.ToString()
	return res, err != nil, err
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx sessionctx.Context, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(chunk.Row{})
	_, err2 := c.Eval(chunk.Row{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// ConstItem implements Expression interface.
func (c *Constant) ConstItem() bool {
	return true
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(c.hashcode) > 0 {
		return c.hashcode
	}
	_, err := c.Eval(chunk.Row{})
	if err != nil {
		terror.Log(err)
	}
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode, err = codec.EncodeValue(sc, c.hashcode, c.Value)
	if err != nil {
		terror.Log(err)
	}
	return c.hashcode
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) (Expression, error) {
	return c, nil
}

func (c *Constant) resolveIndices(_ *Schema) error {
	return nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (c *Constant) Vectorized() bool {
	return true
}
