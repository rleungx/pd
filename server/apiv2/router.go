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

package apiv2

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	gs "github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

var group = server.ServiceGroup{
	Name:       "core",
	IsCore:     true,
	Version:    "v2",
	PathPrefix: apiV2Prefix,
}

const apiV2Prefix = "/pd/api/v2/"

// NewV2Handler creates a HTTP handler for API.
// @title Placement Driver Core API
// @version 2.0
// @description This is placement driver.
// @contact.name Placement Driver Support
// @contact.url https://github.com/tikv/pd/issues
// @contact.email info@pingcap.com
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /pd/api/v2
func NewV2Handler(_ context.Context, svr *server.Server) (http.Handler, server.ServiceGroup, error) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("server", svr)
		c.Next()
	})
	router.Use(middlewares.Redirector())
	root := router.Group(apiV2Prefix)
	meta := root.Group("meta")
	meta.Use(middlewares.BootstrapChecker())
	meta.GET("/stores", handlers.GetStores())
	meta.GET("/stores/:id", handlers.GetStoreByID())
	meta.DELETE("/stores/:id", handlers.DeleteStoreByID())

	root.GET("/members", handlers.GetMembers())
	router.GET("/swagger/*any", gs.WrapHandler(swaggerFiles.Handler))

	return router, group, nil
}
