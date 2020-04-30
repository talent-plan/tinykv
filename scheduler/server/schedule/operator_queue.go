// Copyright 2019 PingCAP, Inc.
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

package schedule

import (
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
)

type operatorWithTime struct {
	op   *operator.Operator
	time time.Time
}

type operatorQueue []*operatorWithTime

func (opn operatorQueue) Len() int { return len(opn) }

func (opn operatorQueue) Less(i, j int) bool {
	return opn[i].time.Before(opn[j].time)
}

func (opn operatorQueue) Swap(i, j int) {
	opn[i], opn[j] = opn[j], opn[i]
}

func (opn *operatorQueue) Push(x interface{}) {
	item := x.(*operatorWithTime)
	*opn = append(*opn, item)
}

func (opn *operatorQueue) Pop() interface{} {
	old := *opn
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*opn = old[0 : n-1]
	return item
}
