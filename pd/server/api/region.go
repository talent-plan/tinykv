// Copyright 2016 PingCAP, Inc.
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

package api

import (
	"container/heap"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/unrolled/render"
)

// RegionInfo records detail region info for api usage.
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []*metapb.Peer      `json:"peers,omitempty"`

	Leader          *metapb.Peer      `json:"leader,omitempty"`
	DownPeers       []*pdpb.PeerStats `json:"down_peers,omitempty"`
	PendingPeers    []*metapb.Peer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64            `json:"written_bytes,omitempty"`
	ReadBytes       uint64            `json:"read_bytes,omitempty"`
	WrittenKeys     uint64            `json:"written_keys,omitempty"`
	ReadKeys        uint64            `json:"read_keys,omitempty"`
	ApproximateSize int64             `json:"approximate_size,omitempty"`
	ApproximateKeys int64             `json:"approximate_keys,omitempty"`
}

// NewRegionInfo create a new api RegionInfo.
func NewRegionInfo(r *core.RegionInfo) *RegionInfo {
	if r == nil {
		return nil
	}
	return &RegionInfo{
		ID:              r.GetID(),
		StartKey:        string(core.HexRegionKey(r.GetStartKey())),
		EndKey:          string(core.HexRegionKey(r.GetEndKey())),
		RegionEpoch:     r.GetRegionEpoch(),
		Peers:           r.GetPeers(),
		Leader:          r.GetLeader(),
		DownPeers:       r.GetDownPeers(),
		PendingPeers:    r.GetPendingPeers(),
		WrittenBytes:    r.GetBytesWritten(),
		WrittenKeys:     r.GetKeysWritten(),
		ReadBytes:       r.GetBytesRead(),
		ReadKeys:        r.GetKeysRead(),
		ApproximateSize: r.GetApproximateSize(),
		ApproximateKeys: r.GetApproximateKeys(),
	}
}

// RegionsInfo contains some regions with the detailed region info.
type RegionsInfo struct {
	Count   int           `json:"count"`
	Regions []*RegionInfo `json:"regions"`
}

type regionHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionHandler(svr *server.Server, rd *render.Render) *regionHandler {
	return &regionHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *regionHandler) GetRegionByID(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	regionInfo := cluster.GetRegion(regionID)
	h.rd.JSON(w, http.StatusOK, NewRegionInfo(regionInfo))
}

func (h *regionHandler) GetRegionByKey(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	vars := mux.Vars(r)
	key := vars["key"]
	regionInfo := cluster.GetRegionInfoByKey([]byte(key))
	h.rd.JSON(w, http.StatusOK, NewRegionInfo(regionInfo))
}

type regionsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionsHandler(svr *server.Server, rd *render.Render) *regionsHandler {
	return &regionsHandler{
		svr: svr,
		rd:  rd,
	}
}

func convertToAPIRegions(regions []*core.RegionInfo) *RegionsInfo {
	regionInfos := make([]*RegionInfo, len(regions))
	for i, r := range regions {
		regionInfos[i] = NewRegionInfo(r)
	}
	return &RegionsInfo{
		Count:   len(regions),
		Regions: regionInfos,
	}
}

func (h *regionsHandler) GetAll(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	regions := cluster.GetRegions()
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) ScanRegions(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	startKey := r.URL.Query().Get("key")

	limit := defaultRegionLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}
	regions := cluster.ScanRegions([]byte(startKey), nil, limit)
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetRegionCount(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	count := cluster.GetRegionCount()
	h.rd.JSON(w, http.StatusOK, &RegionsInfo{Count: count})
}

func (h *regionsHandler) GetStoreRegions(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	regions := cluster.GetStoreRegions(uint64(id))
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetMissPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetMissPeerRegions()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetExtraPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetExtraPeerRegions()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetPendingPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetPendingPeerRegions()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetDownPeerRegions(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetDownPeerRegions()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetOfflinePeer(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetOfflinePeer()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetEmptyRegion(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	regions, err := handler.GetEmptyRegion()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

func (h *regionsHandler) GetRegionSiblings(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	region := cluster.GetRegion(uint64(id))
	if region == nil {
		h.rd.JSON(w, http.StatusNotFound, server.ErrRegionNotFound(uint64(id)).Error())
		return
	}

	left, right := cluster.GetAdjacentRegions(region)
	regionsInfo := convertToAPIRegions([]*core.RegionInfo{left, right})
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

const (
	defaultRegionLimit = 16
	maxRegionLimit     = 10240
)

func (h *regionsHandler) GetTopWriteFlow(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() })
}

func (h *regionsHandler) GetTopReadFlow(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() })
}

func (h *regionsHandler) GetTopConfVer(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	})
}

func (h *regionsHandler) GetTopVersion(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	})
}

func (h *regionsHandler) GetTopSize(w http.ResponseWriter, r *http.Request) {
	h.GetTopNRegions(w, r, func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	})
}

func (h *regionsHandler) GetTopNRegions(w http.ResponseWriter, r *http.Request, less func(a, b *core.RegionInfo) bool) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	limit := defaultRegionLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if limit > maxRegionLimit {
		limit = maxRegionLimit
	}
	regions := TopNRegions(cluster.GetRegions(), less, limit)
	regionsInfo := convertToAPIRegions(regions)
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

// RegionHeap implements heap.Interface, used for selecting top n regions.
type RegionHeap struct {
	regions []*core.RegionInfo
	less    func(a, b *core.RegionInfo) bool
}

func (h *RegionHeap) Len() int           { return len(h.regions) }
func (h *RegionHeap) Less(i, j int) bool { return h.less(h.regions[i], h.regions[j]) }
func (h *RegionHeap) Swap(i, j int)      { h.regions[i], h.regions[j] = h.regions[j], h.regions[i] }

// Push pushes an element x onto the heap.
func (h *RegionHeap) Push(x interface{}) {
	h.regions = append(h.regions, x.(*core.RegionInfo))
}

// Pop removes the minimum element (according to Less) from the heap and returns
// it.
func (h *RegionHeap) Pop() interface{} {
	pos := len(h.regions) - 1
	x := h.regions[pos]
	h.regions = h.regions[:pos]
	return x
}

// Min returns the minimum region from the heap.
func (h *RegionHeap) Min() *core.RegionInfo {
	if h.Len() == 0 {
		return nil
	}
	return h.regions[0]
}

// TopNRegions returns top n regions according to the given rule.
func TopNRegions(regions []*core.RegionInfo, less func(a, b *core.RegionInfo) bool, n int) []*core.RegionInfo {
	if n <= 0 {
		return nil
	}

	hp := &RegionHeap{
		regions: make([]*core.RegionInfo, 0, n),
		less:    less,
	}
	for _, r := range regions {
		if hp.Len() < n {
			heap.Push(hp, r)
			continue
		}
		if less(hp.Min(), r) {
			heap.Pop(hp)
			heap.Push(hp, r)
		}
	}

	res := make([]*core.RegionInfo, hp.Len())
	for i := hp.Len() - 1; i >= 0; i-- {
		res[i] = heap.Pop(hp).(*core.RegionInfo)
	}
	return res
}
