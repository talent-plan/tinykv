// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinStrcmpSig{}
)

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.Flag) || !types.IsNonBinaryStr(argTp) {
		resTp.Flag |= mysql.BinaryFlag
	}
}

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.tp.Flen = 10
	sig := &builtinLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Length)
	return sig, nil
}

type builtinLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinLengthSig) Clone() builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 2
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Strcmp)
	return sig, nil
}

type builtinStrcmpSig struct {
	baseBuiltinFunc
}

func (b *builtinStrcmpSig) Clone() builtinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row chunk.Row) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	right, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := types.CompareString(left, right)
	return int64(res), false, nil
}
