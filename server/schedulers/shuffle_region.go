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

package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

func init() {
	schedule.RegisterScheduler("shuffle-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newShuffleRegionScheduler(opController), nil
	})
}

type shuffleRegionScheduler struct {
	*baseScheduler
	selector *schedule.RandomSelector
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []schedule.Filter{schedule.StoreStateFilter{MoveRegion: true}}
	base := newBaseScheduler(opController)
	return &shuffleRegionScheduler{
		baseScheduler: base,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return "shuffle-region-scheduler"
}

func (s *shuffleRegionScheduler) GetType() string {
	return "shuffle-region"
}

func (s *shuffleRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorInflight("shuffle-region") < cluster.GetMaxShuffleRegionInflight()
}

func (s *shuffleRegionScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := s.scheduleRemovePeer(cluster)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}

	excludedFilter := schedule.NewExcludedFilter(nil, region.GetStoreIds())
	newPeer := s.scheduleAddPeer(cluster, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_new_peer").Inc()
		return nil
	}

	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	op := schedule.CreateMovePeerOperator("shuffle-region", cluster, region, schedule.OpAdmin, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	op.SetPriorityLevel(core.HighPriority)
	return []*schedule.Operator{op}
}

func (s *shuffleRegionScheduler) scheduleRemovePeer(cluster schedule.Cluster) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()

	source := s.selector.SelectSource(cluster, stores)
	if source == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_store").Inc()
		return nil, nil
	}

	region := cluster.RandFollowerRegion(source.GetId(), core.HealthRegion())
	if region == nil {
		region = cluster.RandLeaderRegion(source.GetId(), core.HealthRegion())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}

func (s *shuffleRegionScheduler) scheduleAddPeer(cluster schedule.Cluster, filter schedule.Filter) *metapb.Peer {
	stores := cluster.GetStores()

	target := s.selector.SelectTarget(cluster, stores, filter)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newPeer
}
