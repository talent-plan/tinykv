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
	"sync/atomic"

	"github.com/pingcap/go-ycsb/pkg/util"
)

const (
	// WindowSize is the size of window of pending acks.
	WindowSize int64 = 1 << 20

	// WindowMask is used to turn an ID into a slot in the window.
	WindowMask int64 = WindowSize - 1
)

// AcknowledgedCounter reports generated integers via Last only
// after they have been acknoledged.
type AcknowledgedCounter struct {
	c Counter

	lock util.SpinLock

	window []bool
	limit  int64
}

// NewAcknowledgedCounter creates the counter which starts at start.
func NewAcknowledgedCounter(start int64) *AcknowledgedCounter {
	return &AcknowledgedCounter{
		c:      Counter{counter: start},
		lock:   util.SpinLock{},
		window: make([]bool, WindowSize),
		limit:  start - 1,
	}
}

// Next implements the Generator Next interface.
func (a *AcknowledgedCounter) Next(r *rand.Rand) int64 {
	return a.c.Next(r)
}

// Last implements the Generator Last interface.
func (a *AcknowledgedCounter) Last() int64 {
	return atomic.LoadInt64(&a.limit)
}

// Acknowledge makes a generated counter vaailable via Last.
func (a *AcknowledgedCounter) Acknowledge(value int64) {
	currentSlot := value & WindowMask
	if a.window[currentSlot] {
		panic("Too many unacknowledged insertion keys.")
	}

	a.window[currentSlot] = true

	if !a.lock.TryLock() {
		return
	}

	defer a.lock.Unlock()

	// move a contiguous sequence from the window
	// over to the "limit" variable

	limit := atomic.LoadInt64(&a.limit)
	beforeFirstSlot := limit & WindowMask
	index := limit + 1
	for ; index != beforeFirstSlot; index++ {
		slot := index & WindowMask
		if !a.window[slot] {
			break
		}

		a.window[slot] = false
	}

	atomic.StoreInt64(&a.limit, index-1)
}
