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
	"math/rand"

	"github.com/pingcap/go-ycsb/pkg/util"
)

// ScrambledZipfian produces a sequence of items, such that some items are more popular than
// others, according to a zipfian distribution
type ScrambledZipfian struct {
	Number
	gen       *Zipfian
	min       int64
	max       int64
	itemCount int64
}

// NewScrambledZipfian creates a ScrambledZipfian generator.
func NewScrambledZipfian(min int64, max int64, zipfianConstant float64) *ScrambledZipfian {
	const (
		zetan               = float64(26.46902820178302)
		usedZipfianConstant = float64(0.99)
		itemCount           = int64(10000000000)
	)

	s := new(ScrambledZipfian)
	s.min = min
	s.max = max
	s.itemCount = max - min + 1
	if zipfianConstant == usedZipfianConstant {
		s.gen = NewZipfian(0, itemCount, zipfianConstant, zetan)
	} else {
		s.gen = NewZipfianWithRange(0, itemCount, zipfianConstant)
	}
	return s
}

// Next implements the Generator Next interface.
func (s *ScrambledZipfian) Next(r *rand.Rand) int64 {
	n := s.gen.Next(r)

	n = s.min + util.Hash64(n)%s.itemCount
	s.SetLastValue(n)
	return n
}
