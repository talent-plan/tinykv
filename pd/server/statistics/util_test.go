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

package statistics

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMovingAvg{})

type testMovingAvg struct{}

func addRandData(ma MovingAvg, n int, mx float64) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		ma.Add(rand.Float64() * mx)
	}
}

// checkReset checks the Reset works properly.
// emptyValue is the moving average of empty data set.
func (t *testMovingAvg) checkReset(c *C, ma MovingAvg, emptyValue float64) {
	addRandData(ma, 100, 1000)
	ma.Reset()
	c.Assert(ma.Get(), Equals, emptyValue)
}

// checkAddGet checks Add works properly.
func (t *testMovingAvg) checkAdd(c *C, ma MovingAvg, data []float64, expected []float64) {
	c.Assert(len(data), Equals, len(expected))
	for i, x := range data {
		ma.Add(x)
		c.Assert(ma.Get(), Equals, expected[i])
	}
}

// checkSet checks Set = Reset + Add
func (t *testMovingAvg) checkSet(c *C, ma MovingAvg, data []float64, expected []float64) {
	c.Assert(len(data), Equals, len(expected))

	// Reset + Add
	addRandData(ma, 100, 1000)
	ma.Reset()
	t.checkAdd(c, ma, data, expected)

	// Set
	addRandData(ma, 100, 1000)
	ma.Set(data[0])
	c.Assert(ma.Get(), Equals, expected[0])
	t.checkAdd(c, ma, data[1:], expected[1:])
}

func (t *testMovingAvg) TestMedianFilter(c *C) {
	var empty float64 = 0
	data := []float64{2, 4, 2, 800, 600, 6, 3}
	expected := []float64{2, 3, 2, 3, 4, 6, 6}

	mf := NewMedianFilter(5)
	c.Assert(mf.Get(), Equals, empty)

	t.checkReset(c, mf, empty)
	t.checkAdd(c, mf, data, expected)
	t.checkSet(c, mf, data, expected)
}
