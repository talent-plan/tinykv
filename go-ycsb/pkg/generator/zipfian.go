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
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/go-ycsb/pkg/util"
)

const (
	// ZipfianConstant is the default constant for the zipfian.
	ZipfianConstant = float64(0.99)
)

// Zipfian generates the zipfian distribution. It produces a sequence of items, such that some items are more popular than
// others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
// of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
// itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
// you construct the instance, you can change the number of items by calling Next(*rand.Rand).
//
// Note that the popular items will be clustered together, e.g. item 0 is the most popular, item 1 the second most
// popular, and so on (or min is the most popular, min+1 the next most popular, etc.) If you don't want this clustering,
// and instead want the popular items scattered throughout the item space, then use ScrambledZipfian(located in scrambled_zipfian.go) instead.
//
// Be aware: initializing this generator may take a long time if there are lots of items to choose from (e.g. over a
// minute for 100 million objects). This is because certain mathematical values need to be computed to properly
// generate a zipfian skew, and one of those values (zeta) is a sum sequence from 1 to n, where n is the itemcount.
// Note that if you increase the number of items in the set, we can compute a new zeta incrementally, so it should be
// fast unless you have added millions of items. However, if you decrease the number of items, we recompute zeta from
// scratch, so this can take a long time.
//
// The algorithm used here is from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
type Zipfian struct {
	Number

	lock util.SpinLock

	items int64
	base  int64

	zipfianConstant float64

	alpha      float64
	zetan      float64
	theta      float64
	eta        float64
	zeta2Theta float64

	countForZeta int64

	allowItemCountDecrease bool
}

// NewZipfianWithItems creates the Zipfian generator.
func NewZipfianWithItems(items int64, zipfianConstant float64) *Zipfian {
	return NewZipfianWithRange(0, items-1, zipfianConstant)
}

// NewZipfianWithRange creates the Zipfian generator.
func NewZipfianWithRange(min int64, max int64, zipfianConstant float64) *Zipfian {
	return NewZipfian(min, max, zipfianConstant, zetaStatic(0, max-min+1, zipfianConstant, 0))
}

// NewZipfian creates the Zipfian generator.
func NewZipfian(min int64, max int64, zipfianConstant float64, zetan float64) *Zipfian {
	items := max - min + 1
	z := new(Zipfian)

	z.items = items
	z.base = min

	z.zipfianConstant = zipfianConstant
	theta := z.zipfianConstant
	z.theta = theta

	z.zeta2Theta = z.zeta(0, 2, theta, 0)

	z.alpha = 1.0 / (1.0 - theta)
	z.zetan = zetan
	z.countForZeta = items
	z.eta = (1 - math.Pow(2.0/float64(items), 1-theta)) / (1 - z.zeta2Theta/z.zetan)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z.Next(r)
	return z
}

func (z *Zipfian) zeta(st int64, n int64, thetaVal float64, initialSum float64) float64 {
	z.countForZeta = n
	return zetaStatic(st, n, thetaVal, initialSum)
}

func zetaStatic(st int64, n int64, theta float64, initialSum float64) float64 {
	sum := initialSum

	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}

	return sum
}

func (z *Zipfian) next(r *rand.Rand, itemCount int64) int64 {
	if itemCount != z.countForZeta {
		z.lock.Lock()
		if itemCount > z.countForZeta {
			//we have added more items. can compute zetan incrementally, which is cheaper
			z.zetan = z.zeta(z.countForZeta, itemCount, z.theta, z.zetan)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		} else if itemCount < z.countForZeta && z.allowItemCountDecrease {
			//note : for large itemsets, this is very slow. so don't do it!
			fmt.Printf("recomputing Zipfian distribution, should be avoided,item count %v, count for zeta %v\n", itemCount, z.countForZeta)
			z.zetan = z.zeta(0, itemCount, z.theta, 0)
			z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta)) / (1 - z.zeta2Theta/z.zetan)
		}
		z.lock.Unlock()
	}

	u := r.Float64()
	uz := u * z.zetan

	if uz < 1.0 {
		return z.base
	}

	if uz < 1.0+math.Pow(0.5, z.theta) {
		return z.base + 1
	}

	ret := z.base + int64(float64(itemCount)*math.Pow(z.eta*u-z.eta+1, z.alpha))
	z.SetLastValue(ret)
	return ret
}

// Next implements the Generator Next interface.
func (z *Zipfian) Next(r *rand.Rand) int64 {
	return z.next(r, z.items)
}
