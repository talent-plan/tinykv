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

type discretePair struct {
	Weight float64
	Value  int64
}

// Discrete generates a distribution by choosing from a discrete set of values.
type Discrete struct {
	Number
	values []discretePair
}

// NewDiscrete creates the generator.
func NewDiscrete() *Discrete {
	return &Discrete{}
}

// Next implements the Generator Next interface.
func (d *Discrete) Next(r *rand.Rand) int64 {
	sum := float64(0)

	for _, p := range d.values {
		sum += p.Weight
	}

	val := r.Float64()

	for _, p := range d.values {
		pw := p.Weight / sum
		if val < pw {
			d.SetLastValue(p.Value)
			return p.Value
		}

		val -= pw
	}

	panic("oops, should not get here.")
}

// Add adds a value with weight.
func (d *Discrete) Add(weight float64, value int64) {
	d.values = append(d.values, discretePair{Weight: weight, Value: value})
}
