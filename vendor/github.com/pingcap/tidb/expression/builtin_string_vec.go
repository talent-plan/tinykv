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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinStringIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinStringIsNullSig) vectorized() bool {
	return true
}

func (b *builtinStrcmpSig) vectorized() bool {
	return true
}

func (b *builtinStrcmpSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	leftBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(leftBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, leftBuf); err != nil {
		return err
	}
	rightBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rightBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, rightBuf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(leftBuf, rightBuf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		// if left or right is null, then set to null and return 0(which is the default value)
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(types.CompareString(leftBuf.GetString(i), rightBuf.GetString(i)))
	}
	return nil
}

func (b *builtinLengthSig) vectorized() bool {
	return false
}

// vecEvalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	/* Your code here */
	return nil
}
