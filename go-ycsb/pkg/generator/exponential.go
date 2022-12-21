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

import (
	"math"
	"math/rand"
)

// Exponential generates an exponential distribution. It produces a sequence
// of time intervals according to an expontential distribution.
type Exponential struct {
	Number
	gamma float64
}

// NewExponentialWithMean creates an exponential generator with a mean arrival rate
// of gamma.
func NewExponentialWithMean(mean float64) *Exponential {
	return &Exponential{gamma: 1.0 / mean}
}

// NewExponential creats an exponential generator with percential and range.
func NewExponential(percentile float64, rng float64) *Exponential {
	gamma := -math.Log(1.0-percentile/100.0) / rng
	return &Exponential{
		gamma: gamma,
	}
}

// Next implements the Generator Next interface.
func (e *Exponential) Next(r *rand.Rand) int64 {
	v := int64(-math.Log(r.Float64()) / e.gamma)
	e.SetLastValue(v)
	return v
}
