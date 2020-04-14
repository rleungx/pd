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
	"reflect"
	"strings"

	"github.com/pingcap/errcode"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
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

// @Tags config
// @Summary Get full config.
// @Produce json
// @Success 200 {object} config.Config
// @Router /config [get]
func (h *confHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetConfig())
}

// @Tags config
// @Summary Get default config.
// @Produce json
// @Success 200 {object} config.Config
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /config/default [get]
func (h *confHandler) GetDefault(w http.ResponseWriter, r *http.Request) {
	config := config.NewConfig()
	err := config.Adjust(nil)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.JSON(w, http.StatusOK, config)
}

// FIXME: details of input json body params
// @Tags config
// @Summary Update a config item.
// @Accept json
// @Param body body object false "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config [post]
func (h *confHandler) Post(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetConfig()
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found1, err := h.updateSchedule(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found2, err := h.updateReplication(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	found3, err := h.updatePDServerConfig(data, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found1 && !found2 && !found3 {
		h.rd.JSON(w, http.StatusBadRequest, "config item not found")
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) updateSchedule(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.Schedule, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetScheduleConfig(config.Schedule)
	}
	return found, err
}

func (h *confHandler) updateReplication(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.Replication, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetReplicationConfig(config.Replication)
	}
	return found, err
}

func (h *confHandler) updatePDServerConfig(data []byte, config *config.Config) (bool, error) {
	updated, found, err := h.mergeConfig(&config.PDServerCfg, data)
	if err != nil {
		return false, err
	}
	if updated {
		err = h.svr.SetPDServerConfig(config.PDServerCfg)
	}
	return found, err
}

func (h *confHandler) mergeConfig(v interface{}, data []byte) (updated bool, found bool, err error) {
	old, _ := json.Marshal(v)
	if err := json.Unmarshal(data, v); err != nil {
		return false, false, err
	}
	new, _ := json.Marshal(v)
	if !bytes.Equal(old, new) {
		return true, true, nil
	}
	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		return false, false, err
	}
	t := reflect.TypeOf(v).Elem()
	for i := 0; i < t.NumField(); i++ {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if _, ok := m[jsonTag]; ok {
			return false, true, nil
		}
	}
	return false, false, nil
}

// @Tags config
// @Summary Get schedule config.
// @Produce json
// @Success 200 {object} config.ScheduleConfig
// @Router /config/schedule [get]
func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetScheduleConfig())
}

// @Tags config
// @Summary Update a schedule config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/schedule [post]
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

// @Tags config
// @Summary Get replication config.
// @Produce json
// @Success 200 {object} config.ReplicationConfig
// @Router /config/replicate [get]
func (h *confHandler) GetReplication(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetReplicationConfig())
}

// @Tags config
// @Summary Update a replication config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/replicate [post]
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

// @Tags config
// @Summary Get label property config.
// @Produce json
// @Success 200 {object} config.LabelPropertyConfig
// @Failure 400 {string} string "The input is invalid."
// @Router /config/label-property [get]
func (h *confHandler) GetLabelProperty(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLabelProperty())
}

// @Tags config
// @Summary Update label property config item.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string "The config is updated."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/label-property [post]
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

// @Tags config
// @Summary Get cluster version.
// @Produce json
// @Success 200 {object} semver.Version
// @Router /config/cluster-version [get]
func (h *confHandler) GetClusterVersion(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetClusterVersion())
}

// @Tags config
// @Summary Update cluster version.
// @Accept json
// @Param body body object string "json params"
// @Produce json
// @Success 200 {string} string
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /config/cluster-version [post]
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
