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

package api

import (
	"net/http"

	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/unrolled/render"
)

type healthHandler struct {
	svr *server.Server
	rd  *render.Render
}

// Health reflects the cluster's health.
type Health struct {
	Name       string   `json:"name"`
	MemberID   uint64   `json:"member_id"`
	ClientUrls []string `json:"client_urls"`
	Health     bool     `json:"health"`
}

func newHealthHandler(svr *server.Server, rd *render.Render) *healthHandler {
	return &healthHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *healthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()
	members, err := server.GetMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	unhealthMembers := h.svr.CheckHealth(members)
	healths := []Health{}
	for _, member := range members {
		h := Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     true,
		}
		if _, ok := unhealthMembers[member.GetMemberId()]; ok {
			h.Health = false
		}
		healths = append(healths, h)
	}
	h.rd.JSON(w, http.StatusOK, healths)
}
