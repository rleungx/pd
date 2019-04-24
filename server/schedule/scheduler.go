// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo
	RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo
	GetAverageRegionSize() int64

	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
	GetRegion(id uint64) *core.RegionInfo
	GetRegionStores(region *core.RegionInfo) []*core.StoreInfo
	GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo
	GetLeaderStore(region *core.RegionInfo) *core.StoreInfo
	GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo)
	ScanRegions(startKey []byte, limit int) []*core.RegionInfo

	BlockStore(id uint64) error
	UnblockStore(id uint64)

	OverloadStore(id uint64) error
	UnburdenStore(id uint64)

	IsRegionHot(id uint64) bool
	RegionWriteStats() []*core.RegionStat
	RegionReadStats() []*core.RegionStat
	RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo

	// get config methods
	GetOpt() NamespaceOptions
	Options

	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster Cluster) error
	Cleanup(cluster Cluster)
	Schedule(cluster Cluster) []*Operator
	IsScheduleAllowed(cluster Cluster) bool
}

// CreateSchedulerFunc is for creating scheudler.
type CreateSchedulerFunc func(opController *OperatorController, args []string) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(name string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[name]; ok {
		log.Fatal("duplicated scheduler", zap.String("name", name))
	}
	schedulerMap[name] = createFn
}

// IsSchedulerRegistered check where the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(name string, opController *OperatorController, args ...string) (Scheduler, error) {
	fn, ok := schedulerMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(opController, args)
}
