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
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"

	"github.com/pingcap/go-ycsb/pkg/util"
)

// Histogram generates integers according to a histogram distribution.
type Histogram struct {
	Number
	blockSize    int64
	buckets      []int64
	area         int64
	weightedArea int64
}

// NewHistogram creates a Histogram generator.
func NewHistogram(buckets []int64, blockSize int64) *Histogram {
	var (
		area         int64
		weightedArea int64
	)

	for i, b := range buckets {
		area += b
		weightedArea += int64(i) * b
	}

	return &Histogram{
		buckets:      buckets,
		blockSize:    blockSize,
		area:         area,
		weightedArea: weightedArea,
	}
}

type bucketInfo struct {
	location int64
	value    int64
}

// NewHistogramFromFile creates a Histogram generator from file.
func NewHistogramFromFile(name string) *Histogram {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		util.Fatalf("load histogram file %s failed %v", name, err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	line := strings.Split(strings.TrimSpace(lines[0]), "\t")
	if line[0] != "BlockSize" {
		util.Fatalf("First line of histogram is not the BlockSize but %s", line)
	}

	blockSize, err := strconv.ParseInt(line[1], 10, 64)
	if err != nil {
		util.Fatalf("parse BlockSize failed %v", err)
	}

	ay := make([]bucketInfo, 0, len(lines[1:]))
	maxLocation := int64(0)
	for _, s := range lines[1:] {
		s = strings.TrimSpace(s)
		if len(s) == 0 {
			break
		}

		line = strings.Split(s, "\t")
		location, _ := strconv.ParseInt(line[0], 10, 64)
		if maxLocation < location {
			maxLocation = location
		}
		value, _ := strconv.ParseInt(line[1], 10, 64)
		ay = append(ay, bucketInfo{location: location, value: value})
	}

	buckets := make([]int64, maxLocation+1)
	for _, b := range ay {
		buckets[b.location] = b.value
	}

	return NewHistogram(buckets, blockSize)
}

// Next implements the Generator Next interface.
func (h *Histogram) Next(r *rand.Rand) int64 {
	n := r.Int63n(h.area)

	i := int64(0)
	for ; i < int64(len(h.buckets))-1; i++ {
		n -= h.buckets[i]
		if n <= 0 {
			return (i + 1) * h.blockSize
		}
	}

	v := i * h.blockSize
	h.SetLastValue(v)
	return v
}
