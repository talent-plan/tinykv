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

package syncer

import (
	"strconv"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"go.uber.org/zap"
)

const (
	historyKey        = "historyIndex"
	defaultFlushCount = 100
)

type historyBuffer struct {
	sync.RWMutex
	index      uint64
	records    []*core.RegionInfo
	head       int
	tail       int
	size       int
	kv         kv.Base
	flushCount int
}

func newHistoryBuffer(size int, kv kv.Base) *historyBuffer {
	// use an empty space to simplify operation
	size++
	if size < 2 {
		size = 2
	}
	records := make([]*core.RegionInfo, size)
	h := &historyBuffer{
		records:    records,
		size:       size,
		kv:         kv,
		flushCount: defaultFlushCount,
	}
	h.reload()
	return h
}

func (h *historyBuffer) len() int {
	return h.distanceToTail(h.head)
}

func (h *historyBuffer) distanceToTail(pos int) int {
	if h.tail < pos {
		return h.tail + h.size - pos
	}
	return h.tail - pos

}

func (h *historyBuffer) nextIndex() uint64 {
	return h.index
}

func (h *historyBuffer) firstIndex() uint64 {
	return h.index - uint64(h.len())
}

func (h *historyBuffer) Record(r *core.RegionInfo) {
	h.Lock()
	defer h.Unlock()
	regionSyncerStatus.WithLabelValues("sync_index").Set(float64(h.index))
	h.records[h.tail] = r
	h.tail = (h.tail + 1) % h.size
	if h.tail == h.head {
		h.head = (h.head + 1) % h.size
	}
	h.index++
	h.flushCount--
	if h.flushCount <= 0 {
		h.persist()
		h.flushCount = defaultFlushCount
	}
}

func (h *historyBuffer) RecordsFrom(index uint64) []*core.RegionInfo {
	h.RLock()
	defer h.RUnlock()
	var pos int
	if index < h.nextIndex() && index >= h.firstIndex() {
		pos = (h.head + int(index-h.firstIndex())) % h.size
	} else {
		return nil
	}
	records := make([]*core.RegionInfo, 0, h.distanceToTail(pos))
	for i := pos; i != h.tail; i = (i + 1) % h.size {
		records = append(records, h.records[i])
	}
	return records
}

func (h *historyBuffer) ResetWithIndex(index uint64) {
	h.Lock()
	defer h.Unlock()
	h.index = index
	h.head = 0
	h.tail = 0
	h.flushCount = defaultFlushCount
}

func (h *historyBuffer) GetNextIndex() uint64 {
	h.RLock()
	defer h.RUnlock()
	return h.index
}

func (h *historyBuffer) get(index uint64) *core.RegionInfo {
	if index < h.nextIndex() && index >= h.firstIndex() {
		pos := (h.head + int(index-h.firstIndex())) % h.size
		return h.records[pos]
	}
	return nil
}

func (h *historyBuffer) reload() {
	v, err := h.kv.Load(historyKey)
	if err != nil {
		log.Warn("load history index failed", zap.String("error", err.Error()))
	}
	if v != "" {
		h.index, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatal("load history index failed", zap.Error(err))
		}
	}
	log.Info("start from history index", zap.Uint64("start-index", h.firstIndex()))
}

func (h *historyBuffer) persist() {
	regionSyncerStatus.WithLabelValues("first_index").Set(float64(h.firstIndex()))
	regionSyncerStatus.WithLabelValues("last_index").Set(float64(h.nextIndex()))
	err := h.kv.Save(historyKey, strconv.FormatUint(h.nextIndex(), 10))
	if err != nil {
		log.Warn("persist history index failed", zap.Uint64("persist-index", h.nextIndex()), zap.Error(err))
	}
}
