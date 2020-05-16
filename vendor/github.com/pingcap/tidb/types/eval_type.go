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

package types

import ast "github.com/pingcap/tidb/parser/types"

// EvalType indicates the specified types that arguments and result of a built-in function should be.
type EvalType = ast.EvalType

const (
	// ETInt represents type INT in evaluation.
	ETInt = ast.ETInt
	// ETReal represents type REAL in evaluation.
	ETReal = ast.ETReal
	// ETString represents type STRING in evaluation.
	ETString = ast.ETString
)
