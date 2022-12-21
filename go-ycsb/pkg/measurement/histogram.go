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

package measurement

import (
	"sort"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/pingcap/go-ycsb/pkg/util"
)

type histogram struct {
	boundCounts util.ConcurrentMap
	startTime   time.Time
	hist        *hdrhistogram.Histogram
}

// Metric name.
const (
	ELAPSED   = "ELAPSED"
	COUNT     = "COUNT"
	QPS       = "QPS"
	AVG       = "AVG"
	MIN       = "MIN"
	MAX       = "MAX"
	PER99TH   = "PER99TH"
	PER999TH  = "PER999TH"
	PER9999TH = "PER9999TH"
)

func newHistogram() *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.hist = hdrhistogram.New(1, 24*60*60*1000*1000, 3)
	return h
}

func (h *histogram) Measure(latency time.Duration) {
	h.hist.RecordValue(latency.Microseconds())
}

func (h *histogram) Summary() []string {
	res := h.getInfo()

	return []string{
		util.FloatToOneString(res[ELAPSED]),
		util.IntToString(res[COUNT]),
		util.FloatToOneString(res[QPS]),
		util.IntToString(res[AVG]),
		util.IntToString(res[MIN]),
		util.IntToString(res[MAX]),
		util.IntToString(res[PER99TH]),
		util.IntToString(res[PER999TH]),
		util.IntToString(res[PER9999TH]),
	}
}

func (h *histogram) getInfo() map[string]interface{} {
	min := h.hist.Min()
	max := h.hist.Max()
	avg := int64(h.hist.Mean())
	count := h.hist.TotalCount()

	bounds := h.boundCounts.Keys()
	sort.Ints(bounds)

	per99 := h.hist.ValueAtPercentile(99)
	per999 := h.hist.ValueAtPercentile(99.9)
	per9999 := h.hist.ValueAtPercentile(99.99)

	elapsed := time.Now().Sub(h.startTime).Seconds()
	qps := float64(count) / elapsed
	res := make(map[string]interface{})
	res[ELAPSED] = elapsed
	res[COUNT] = count
	res[QPS] = qps
	res[AVG] = avg
	res[MIN] = min
	res[MAX] = max
	res[PER99TH] = per99
	res[PER999TH] = per999
	res[PER9999TH] = per9999

	return res
}
