// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/cluster"
)

// GetStores returns the stores.
func GetStores() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		stores := rc.GetMetaStores()
		StoresInfo := &StoresInfo{
			Stores: make([]*StoreInfo, 0, len(stores)),
		}

		nodeStates, exist := c.GetQueryArray("node_state")
		for _, s := range stores {
			storeID := s.GetId()
			store := rc.GetStore(storeID)
			if store == nil {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}

			if !exist || (exist && store.IsInStates(nodeStates)) {
				storeInfo := newStoreInfo(rc.GetOpts().GetScheduleConfig(), store)
				StoresInfo.Stores = append(StoresInfo.Stores, storeInfo)
			}
		}
		StoresInfo.Count = len(StoresInfo.Stores)
		c.IndentedJSON(http.StatusOK, StoresInfo)
	}
}

// GetStoreByID returns the store according to the given ID.
// @Tags store
// @version 2.0
// @Summary Get a store's information.
// @Param id path integer true "Store Id"
// @Produce json
// @Success 200 {object} StoreInfo
// @Failure 400 {string} string "The input is invalid."
// @Failure 404 {string} string "The store does not exist."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /stores/{id} [get]
func GetStoreByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error())
			return
		}
		store := rc.GetStore(id)
		if store == nil {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		storeInfo := newStoreInfo(rc.GetOpts().GetScheduleConfig(), store)
		c.IndentedJSON(http.StatusOK, storeInfo)
	}
}

// DeleteStoreByID will delete the store according to the given ID.
func DeleteStoreByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error())
			return
		}

		var force bool
		forceQuery, exist := c.GetQuery("force")
		if exist && forceQuery != "" {
			force, err = strconv.ParseBool(forceQuery)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrStrconvParseBool.Wrap(err).FastGenWithCause().Error())
				return
			}
		}

		err = rc.RemoveStore(id, force)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
			return
		}

		c.JSON(http.StatusOK, nil)
	}
}

type updateStoresParams struct {
	NodeState    string  `json:"node_state"`
	LeaderWeight float64 `json:"leader_weight"`
	RegionWeight float64 `json:"region_weight"`
}

// UpdateStoreByID will delete the store according to the given ID.
func UpdateStoreByID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := c.MustGet("cluster").(*cluster.RaftCluster)
		idParam := c.Param("id")
		id, err := strconv.ParseUint(idParam, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause().Error())
			return
		}

		var p updateStoresParams
		if err := c.BindJSON(&p); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
			return
		}

		if p.LeaderWeight < 0 || p.RegionWeight < 0 {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		if err := rc.SetStoreWeight(id, p.LeaderWeight, p.RegionWeight); err != nil {
			if errors.Is(err, errs.ErrStoreNotFound) {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}

		if p.NodeState != "" {
			switch p.NodeState {
			case metapb.NodeState_Serving.String():
				if err = rc.UpStore(id); errors.Is(err, errs.ErrStoreNotFound) {
					c.AbortWithStatus(http.StatusNotFound)
					return
				}
			case metapb.NodeState_Removing.String():
				if err = rc.RemoveStore(id, false); errors.Is(err, errs.ErrStoreNotFound) {
					c.AbortWithStatus(http.StatusNotFound)
					return
				}
			default:
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
		}

		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}

		c.JSON(http.StatusOK, nil)
	}
}
