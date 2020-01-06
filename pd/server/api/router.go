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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/unrolled/render"
)

const pingAPI = "/ping"

func createRouter(prefix string, svr *server.Server) *mux.Router {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	router := mux.NewRouter().PathPrefix(prefix).Subrouter()
	handler := svr.GetHandler()

	operatorHandler := newOperatorHandler(handler, rd)
	router.HandleFunc("/api/v1/operators", operatorHandler.List).Methods("GET")
	router.HandleFunc("/api/v1/operators", operatorHandler.Post).Methods("POST")
	router.HandleFunc("/api/v1/operators/{region_id}", operatorHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/operators/{region_id}", operatorHandler.Delete).Methods("DELETE")

	schedulerHandler := newSchedulerHandler(handler, rd)
	router.HandleFunc("/api/v1/schedulers", schedulerHandler.List).Methods("GET")
	router.HandleFunc("/api/v1/schedulers", schedulerHandler.Post).Methods("POST")
	router.HandleFunc("/api/v1/schedulers/{name}", schedulerHandler.Delete).Methods("DELETE")
	schedulerConfigHandler := newSchedulerConfigHandler(svr, rd)
	router.PathPrefix(server.ScheduleConfigHandlerPath).Handler(schedulerConfigHandler)

	clusterHandler := newClusterHandler(svr, rd)
	router.Handle("/api/v1/cluster", clusterHandler).Methods("GET")
	router.HandleFunc("/api/v1/cluster/status", clusterHandler.GetClusterStatus).Methods("GET")

	confHandler := newConfHandler(svr, rd)
	router.HandleFunc("/api/v1/config", confHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/config", confHandler.Post).Methods("POST")
	router.HandleFunc("/api/v1/config/schedule", confHandler.SetSchedule).Methods("POST")
	router.HandleFunc("/api/v1/config/schedule", confHandler.GetSchedule).Methods("GET")
	router.HandleFunc("/api/v1/config/replicate", confHandler.SetReplication).Methods("POST")
	router.HandleFunc("/api/v1/config/replicate", confHandler.GetReplication).Methods("GET")
	router.HandleFunc("/api/v1/config/label-property", confHandler.GetLabelProperty).Methods("GET")
	router.HandleFunc("/api/v1/config/label-property", confHandler.SetLabelProperty).Methods("POST")
	router.HandleFunc("/api/v1/config/cluster-version", confHandler.GetClusterVersion).Methods("GET")
	router.HandleFunc("/api/v1/config/cluster-version", confHandler.SetClusterVersion).Methods("POST")

	storeHandler := newStoreHandler(handler, rd)
	router.HandleFunc("/api/v1/store/{id}", storeHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/store/{id}", storeHandler.Delete).Methods("DELETE")
	router.HandleFunc("/api/v1/store/{id}/state", storeHandler.SetState).Methods("POST")
	router.HandleFunc("/api/v1/store/{id}/label", storeHandler.SetLabels).Methods("POST")
	router.HandleFunc("/api/v1/store/{id}/weight", storeHandler.SetWeight).Methods("POST")
	router.HandleFunc("/api/v1/store/{id}/limit", storeHandler.SetLimit).Methods("POST")
	storesHandler := newStoresHandler(handler, rd)
	router.Handle("/api/v1/stores", storesHandler).Methods("GET")
	router.HandleFunc("/api/v1/stores/remove-tombstone", storesHandler.RemoveTombStone).Methods("DELETE")
	router.HandleFunc("/api/v1/stores/limit", storesHandler.GetAllLimit).Methods("GET")
	router.HandleFunc("/api/v1/stores/limit", storesHandler.SetAllLimit).Methods("POST")

	labelsHandler := newLabelsHandler(svr, rd)
	router.HandleFunc("/api/v1/labels", labelsHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/labels/stores", labelsHandler.GetStores).Methods("GET")

	hotStatusHandler := newHotStatusHandler(handler, rd)
	router.HandleFunc("/api/v1/hotspot/regions/write", hotStatusHandler.GetHotWriteRegions).Methods("GET")
	router.HandleFunc("/api/v1/hotspot/regions/read", hotStatusHandler.GetHotReadRegions).Methods("GET")
	router.HandleFunc("/api/v1/hotspot/stores", hotStatusHandler.GetHotStores).Methods("GET")

	regionHandler := newRegionHandler(svr, rd)
	router.HandleFunc("/api/v1/region/id/{id}", regionHandler.GetRegionByID).Methods("GET")
	router.HandleFunc("/api/v1/region/key/{key}", regionHandler.GetRegionByKey).Methods("GET")

	regionsHandler := newRegionsHandler(svr, rd)
	router.HandleFunc("/api/v1/regions", regionsHandler.GetAll).Methods("GET")
	router.HandleFunc("/api/v1/regions/key", regionsHandler.ScanRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/count", regionsHandler.GetRegionCount).Methods("GET")
	router.HandleFunc("/api/v1/regions/store/{id}", regionsHandler.GetStoreRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/writeflow", regionsHandler.GetTopWriteFlow).Methods("GET")
	router.HandleFunc("/api/v1/regions/readflow", regionsHandler.GetTopReadFlow).Methods("GET")
	router.HandleFunc("/api/v1/regions/confver", regionsHandler.GetTopConfVer).Methods("GET")
	router.HandleFunc("/api/v1/regions/version", regionsHandler.GetTopVersion).Methods("GET")
	router.HandleFunc("/api/v1/regions/size", regionsHandler.GetTopSize).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/miss-peer", regionsHandler.GetMissPeerRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/extra-peer", regionsHandler.GetExtraPeerRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/pending-peer", regionsHandler.GetPendingPeerRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/down-peer", regionsHandler.GetDownPeerRegions).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/offline-peer", regionsHandler.GetOfflinePeer).Methods("GET")
	router.HandleFunc("/api/v1/regions/check/empty-region", regionsHandler.GetEmptyRegion).Methods("GET")
	router.HandleFunc("/api/v1/regions/sibling/{id}", regionsHandler.GetRegionSiblings).Methods("GET")

	router.Handle("/api/v1/version", newVersionHandler(rd)).Methods("GET")
	router.Handle("/api/v1/status", newStatusHandler(rd)).Methods("GET")

	memberHandler := newMemberHandler(svr, rd)
	router.HandleFunc("/api/v1/members", memberHandler.ListMembers).Methods("GET")
	router.HandleFunc("/api/v1/members/name/{name}", memberHandler.DeleteByName).Methods("DELETE")
	router.HandleFunc("/api/v1/members/id/{id}", memberHandler.DeleteByID).Methods("DELETE")
	router.HandleFunc("/api/v1/members/name/{name}", memberHandler.SetMemberPropertyByName).Methods("POST")

	leaderHandler := newLeaderHandler(svr, rd)
	router.HandleFunc("/api/v1/leader", leaderHandler.Get).Methods("GET")
	router.HandleFunc("/api/v1/leader/resign", leaderHandler.Resign).Methods("POST")
	router.HandleFunc("/api/v1/leader/transfer/{next_leader}", leaderHandler.Transfer).Methods("POST")

	statsHandler := newStatsHandler(svr, rd)
	router.HandleFunc("/api/v1/stats/region", statsHandler.Region).Methods("GET")

	trendHandler := newTrendHandler(svr, rd)
	router.HandleFunc("/api/v1/trend", trendHandler.Handle).Methods("GET")

	adminHandler := newAdminHandler(svr, rd)
	router.HandleFunc("/api/v1/admin/cache/region/{id}", adminHandler.HandleDropCacheRegion).Methods("DELETE")
	router.HandleFunc("/api/v1/admin/reset-ts", adminHandler.ResetTS).Methods("POST")

	logHanler := newlogHandler(svr, rd)
	router.HandleFunc("/api/v1/admin/log", logHanler.Handle).Methods("POST")

	router.Handle("/api/v1/health", newHealthHandler(svr, rd)).Methods("GET")
	router.Handle("/api/v1/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")

	// Deprecated
	router.Handle("/health", newHealthHandler(svr, rd)).Methods("GET")
	// Deprecated
	router.Handle("/diagnose", newDiagnoseHandler(svr, rd)).Methods("GET")

	router.HandleFunc(pingAPI, func(w http.ResponseWriter, r *http.Request) {}).Methods("GET")

	return router
}
