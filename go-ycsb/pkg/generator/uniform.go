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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package generator

import "math/rand"

// Uniform generates integers randomly.
type Uniform struct {
	Number
	lb       int64
	ub       int64
	interval int64
}

// NewUniform creates the Uniform generator.
func NewUniform(lb int64, ub int64) *Uniform {
	return &Uniform{
		lb:       lb,
		ub:       ub,
		interval: ub - lb + 1,
	}
}

// Next implements the Generator Next interface.
func (u *Uniform) Next(r *rand.Rand) int64 {
	n := r.Int63n(u.interval) + u.lb
	u.SetLastValue(n)
	return n
}
