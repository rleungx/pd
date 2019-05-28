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

package member

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api/util"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

type memberHandler struct {
	svr *server.Server
	rd  *render.Render
}

func NewMemberHandler(svr *server.Server, rd *render.Render) *memberHandler {
	return &memberHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *memberHandler) ListMembers(w http.ResponseWriter, r *http.Request) {
	members, err := h.getMembers()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, members)
}

func (h *memberHandler) getMembers() (*pdpb.GetMembersResponse, error) {
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: h.svr.ClusterID()}}
	members, err := h.svr.GetMembers(context.Background(), req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Fill leader priorities.
	for _, m := range members.GetMembers() {
		if h.svr.GetEtcdLeader() == 0 {
			log.Warn("no etcd leader, skip get leader priority", zap.Uint64("member", m.GetMemberId()))
			continue
		}
		leaderPriority, e := h.svr.GetMemberLeaderPriority(m.GetMemberId())
		if e != nil {
			log.Error("failed to load leader priority", zap.Uint64("member", m.GetMemberId()), zap.Error(err))
			continue
		}
		m.LeaderPriority = int32(leaderPriority)
	}
	return members, nil
}

func (h *memberHandler) DeleteByName(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()

	// Get etcd ID by name.
	var id uint64
	name := mux.Vars(r)["name"]
	listResp, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, m := range listResp.Members {
		if name == m.Name {
			id = m.ID
			break
		}
	}
	if id == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("not found, pd: %s", name))
		return
	}

	// Delete config.
	err = h.svr.DeleteMemberLeaderPriority(id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Remove member by id
	_, err = etcdutil.RemoveEtcdMember(client, id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("removed, pd: %s", name))
}

func (h *memberHandler) DeleteByID(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// Delete config.
	err = h.svr.DeleteMemberLeaderPriority(id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	client := h.svr.GetClient()
	_, err = etcdutil.RemoveEtcdMember(client, id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("removed, pd: %v", id))
}

func (h *memberHandler) SetMemberPropertyByName(w http.ResponseWriter, r *http.Request) {
	members, membersErr := h.getMembers()
	if membersErr != nil {
		h.rd.JSON(w, http.StatusInternalServerError, membersErr.Error())
		return
	}

	var memberID uint64
	name := mux.Vars(r)["name"]
	for _, m := range members.GetMembers() {
		if m.GetName() == name {
			memberID = m.GetMemberId()
			break
		}
	}
	if memberID == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("not found, pd: %s", name))
		return
	}

	var input map[string]interface{}
	if err := util.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	for k, v := range input {
		switch k {
		case "leader-priority":
			priority, ok := v.(float64)
			if !ok {
				h.rd.JSON(w, http.StatusBadRequest, "bad format leader priority")
				return
			}
			err := h.svr.SetMemberLeaderPriority(memberID, int(priority))
			if err != nil {
				h.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	h.rd.JSON(w, http.StatusOK, "success")
}

type leaderHandler struct {
	svr *server.Server
	rd  *render.Render
}

func NewLeaderHandler(svr *server.Server, rd *render.Render) *leaderHandler {
	return &leaderHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *leaderHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLeader())
}

func (h *leaderHandler) Resign(w http.ResponseWriter, r *http.Request) {
	err := h.svr.ResignLeader("")
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *leaderHandler) Transfer(w http.ResponseWriter, r *http.Request) {
	err := h.svr.ResignLeader(mux.Vars(r)["next_leader"])
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}
