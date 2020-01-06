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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/errcode"
	"github.com/pingcap-incubator/tinykv/pd/pkg/apiutil"
	"github.com/pingcap-incubator/tinykv/pd/server"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

type confHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newConfHandler(svr *server.Server, rd *render.Render) *confHandler {
	return &confHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *confHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetConfig())
}

func (h *confHandler) Post(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetConfig()
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	up1, err := h.updateSchedule(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	up2, err := h.updateReplication(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	up3, err := h.updatePDServerConfig(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !up1 && !up2 && !up3 {
		h.rd.JSON(w, http.StatusBadRequest, "config item not found")
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) updateSchedule(data []byte, config *config.Config) (bool, error) {
	old, _ := json.Marshal(config.Schedule)
	if err := json.Unmarshal(data, &config.Schedule); err != nil {
		return false, err
	}
	new, _ := json.Marshal(config.Schedule)
	if bytes.Equal(old, new) {
		return false, nil
	}
	return true, h.svr.SetScheduleConfig(config.Schedule)
}

func (h *confHandler) updateReplication(data []byte, config *config.Config) (bool, error) {
	old, _ := json.Marshal(config.Replication)
	if err := json.Unmarshal(data, &config.Replication); err != nil {
		return false, err
	}
	new, _ := json.Marshal(config.Replication)
	if bytes.Equal(old, new) {
		return false, nil
	}
	return true, h.svr.SetReplicationConfig(config.Replication)
}

func (h *confHandler) updatePDServerConfig(data []byte, config *config.Config) (bool, error) {
	old, _ := json.Marshal(config.PDServerCfg)
	if err := json.Unmarshal(data, &config.PDServerCfg); err != nil {
		return false, err
	}
	new, _ := json.Marshal(config.PDServerCfg)
	if bytes.Equal(old, new) {
		return false, nil
	}
	return true, h.svr.SetPDServerConfig(config.PDServerCfg)
}

func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetScheduleConfig())
}

func (h *confHandler) SetSchedule(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetScheduleConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetScheduleConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetReplication(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationConfig())
}

func (h *confHandler) SetReplication(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetReplicationConfig()
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetReplicationConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetLabelProperty(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

func (h *confHandler) SetLabelProperty(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	var err error
	switch input["action"] {
	case "set":
		err = h.svr.SetLabelProperty(input["type"], input["label-key"], input["label-value"])
	case "delete":
		err = h.svr.DeleteLabelProperty(input["type"], input["label-key"], input["label-value"])
	default:
		err = errors.Errorf("unknown action %v", input["action"])
	}
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetClusterVersion(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

func (h *confHandler) SetClusterVersion(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	version, ok := input["cluster-version"]
	if !ok {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set cluster-version")))
		return
	}
	err := h.svr.SetClusterVersion(version)
	if err != nil {
		apiutil.ErrorResp(h.rd, w, errcode.NewInternalErr(err))
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
