// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinGEIntSig) vectorized() bool {
	return true
}

func (b *builtinGEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGE(result.Int64s())
	return nil
}

func (b *builtinEQIntSig) vectorized() bool {
	return true
}

func (b *builtinEQIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfEQ(result.Int64s())
	return nil
}

func (b *builtinNEIntSig) vectorized() bool {
	return true
}

func (b *builtinNEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfNE(result.Int64s())
	return nil
}

func (b *builtinGTIntSig) vectorized() bool {
	return true
}

func (b *builtinGTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGT(result.Int64s())
	return nil
}

func (b *builtinLEIntSig) vectorized() bool {
	return true
}

func (b *builtinLEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLE(result.Int64s())
	return nil
}

func (b *builtinLTIntSig) vectorized() bool {
	return true
}

func (b *builtinLTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLT(result.Int64s())
	return nil
}

func vecResOfLT(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] < 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfNE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] != 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfEQ(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] == 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfLE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] <= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGT(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] > 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] >= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

//vecCompareInt is vectorized CompareInt()
func vecCompareInt(isUnsigned0, isUnsigned1 bool, largs, rargs, result *chunk.Column) {
	switch {
	case isUnsigned0 && isUnsigned1:
		types.VecCompareUU(largs.Uint64s(), rargs.Uint64s(), result.Int64s())
	case isUnsigned0 && !isUnsigned1:
		types.VecCompareUI(largs.Uint64s(), rargs.Int64s(), result.Int64s())
	case !isUnsigned0 && isUnsigned1:
		types.VecCompareIU(largs.Int64s(), rargs.Uint64s(), result.Int64s())
	case !isUnsigned0 && !isUnsigned1:
		types.VecCompareII(largs.Int64s(), rargs.Int64s(), result.Int64s())
	}
}
