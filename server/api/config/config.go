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

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/errcode"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api/util"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

type confHandler struct {
	svr *server.Server
	rd  *render.Render
}

func NewConfHandler(svr *server.Server, rd *render.Render) *confHandler {
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
	if err := json.Unmarshal(data, &config.Schedule); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := json.Unmarshal(data, &config.Replication); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := json.Unmarshal(data, &config.PDServerCfg); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.svr.SetScheduleConfig(config.Schedule); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.svr.SetReplicationConfig(config.Replication); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.svr.SetPDServerConfig(config.PDServerCfg); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetScheduleConfig())
}

func (h *confHandler) SetSchedule(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetScheduleConfig()
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
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
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetReplicationConfig(*config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if !h.svr.IsNamespaceExist(name) {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("invalid namespace Name %s, not found", name))
		return
	}

	// adjust field that is zero value to global value
	cfg := h.svr.GetNamespaceConfigWithAdjust(name)
	h.rd.JSON(w, http.StatusOK, cfg)
}

func (h *confHandler) SetNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if !h.svr.IsNamespaceExist(name) {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("invalid namespace Name %s, not found", name))
		return
	}

	config := h.svr.GetNamespaceConfig(name)
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &config); err != nil {
		return
	}

	if err := h.svr.SetNamespaceConfig(name, *config); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) DeleteNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if !h.svr.IsNamespaceExist(name) {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("invalid namespace Name %s, not found", name))
		return
	}

	if err := h.svr.DeleteNamespaceConfig(name); err != nil {
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
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
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
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	version, ok := input["cluster-version"]
	if !ok {
		util.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set cluster-version")))
		return
	}
	err := h.svr.SetClusterVersion(version)
	if err != nil {
		util.ErrorResp(h.rd, w, errcode.NewInternalErr(err))
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
