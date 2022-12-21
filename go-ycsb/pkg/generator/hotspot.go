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

// Hotspot generates integers resembling a hotspot distribution where %x of operations
// access y% of data items.
type Hotspot struct {
	Number
	lowerBound     int64
	upperBound     int64
	hotInterval    int64
	coldInterval   int64
	hotsetFraction float64
	hotOpnFraction float64
}

// NewHotspot creates a Hotspot generator.
// lowerBound: the lower bound of the distribution.
// upperBound: the upper bound of the distribution.
// hotsetFraction: percentage of data itme.
// hotOpnFraction: percentage of operations accessing the hot set.
func NewHotspot(lowerBound int64, upperBound int64, hotsetFraction float64, hotOpnFraction float64) *Hotspot {
	if hotsetFraction < 0.0 || hotsetFraction > 1.0 {
		hotsetFraction = 0.0
	}

	if hotOpnFraction < 0.0 || hotOpnFraction > 1.0 {
		hotOpnFraction = 0.0
	}

	if lowerBound > upperBound {
		lowerBound, upperBound = upperBound, lowerBound
	}

	interval := upperBound - lowerBound + 1
	hotInterval := int64(float64(interval) * hotsetFraction)
	return &Hotspot{
		lowerBound:     lowerBound,
		upperBound:     upperBound,
		hotsetFraction: hotsetFraction,
		hotOpnFraction: hotOpnFraction,
		hotInterval:    hotInterval,
		coldInterval:   interval - hotInterval,
	}
}

// Next implements the Generator Next interface.
func (h *Hotspot) Next(r *rand.Rand) int64 {
	value := int64(0)
	if r.Float64() < h.hotOpnFraction {
		value = h.lowerBound + r.Int63n(h.hotInterval)
	} else {
		value = h.lowerBound + h.hotInterval + r.Int63n(h.coldInterval)
	}
	h.SetLastValue(value)
	return value
}
