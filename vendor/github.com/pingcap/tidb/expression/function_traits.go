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
	"github.com/pingcap/tidb/parser/ast"
)

// unFoldableFunctions stores functions which can not be folded duration constant folding stage.
var unFoldableFunctions = map[string]struct{}{
	ast.RowFunc: {},
	ast.SetVar:  {},
	ast.GetVar:  {},
}

// inequalFunctions stores functions which cannot be propagated from column equal condition.
var inequalFunctions = map[string]struct{}{
	ast.IsNull: {},
}

// mutableEffectsFunctions stores functions which are mutable or have side effects, specifically,
// we cannot remove them from filter even if they have duplicates.
var mutableEffectsFunctions = map[string]struct{}{
	ast.SetVar: {},
	ast.GetVar: {},
}
