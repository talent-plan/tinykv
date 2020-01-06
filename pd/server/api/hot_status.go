// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"net/http"

	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/unrolled/render"
)

type hotStatusHandler struct {
	*server.Handler
	rd *render.Render
}

// HotStoreStats is used to record the status of hot stores.
type HotStoreStats struct {
	BytesWriteStats map[uint64]float64 `json:"bytes-write-rate,omitempty"`
	BytesReadStats  map[uint64]float64 `json:"bytes-read-rate,omitempty"`
	KeysWriteStats  map[uint64]float64 `json:"keys-write-rate,omitempty"`
	KeysReadStats   map[uint64]float64 `json:"keys-read-rate,omitempty"`
}

func newHotStatusHandler(handler *server.Handler, rd *render.Render) *hotStatusHandler {
	return &hotStatusHandler{
		Handler: handler,
		rd:      rd,
	}
}

func (h *hotStatusHandler) GetHotWriteRegions(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.Handler.GetHotWriteRegions())
}

func (h *hotStatusHandler) GetHotReadRegions(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.Handler.GetHotReadRegions())
}

func (h *hotStatusHandler) GetHotStores(w http.ResponseWriter, r *http.Request) {
	bytesWriteStats := h.GetHotBytesWriteStores()
	bytesReadStats := h.GetHotBytesReadStores()
	keysWriteStats := h.GetHotKeysWriteStores()
	keysReadStats := h.GetHotKeysReadStores()

	stats := HotStoreStats{
		BytesWriteStats: bytesWriteStats,
		BytesReadStats:  bytesReadStats,
		KeysWriteStats:  keysWriteStats,
		KeysReadStats:   keysReadStats,
	}
	h.rd.JSON(w, http.StatusOK, stats)
}
