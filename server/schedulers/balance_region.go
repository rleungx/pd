// Copyright 2017 TiKV Project Authors.
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

package schedulers

import (
	"sort"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceRegionName
			return nil
		}
	})
	schedule.RegisterScheduler(BalanceRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceRegionScheduler(opController, conf), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	// BalanceRegionName is balance region scheduler name.
	BalanceRegionName = "balance-region-scheduler"
	// BalanceRegionType is balance region scheduler type.
	BalanceRegionType = "balance-region"
)

type balanceRegionSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceRegionScheduler struct {
	*BaseScheduler
	*retryQuota
	conf         *balanceRegionSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, conf *balanceRegionSchedulerConfig, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceRegionScheduler{
		BaseScheduler: base,
		retryQuota:    newRetryQuota(balanceRegionRetryLimit, defaultMinRetryLimit, defaultRetryQuotaAttenuation),
		conf:          conf,
		opController:  opController,
		counter:       balanceRegionCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
		filter.NewSpecialUseFilter(scheduler.GetName()),
	}
	return scheduler
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

// WithBalanceRegionCounter sets the counter for the scheduler.
func WithBalanceRegionCounter(counter *prometheus.CounterVec) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.counter = counter
	}
}

// WithBalanceRegionName sets the name for the scheduler.
func WithBalanceRegionName(name string) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.conf.Name = name
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *balanceRegionScheduler) GetType() string {
	return BalanceRegionType
}

func (s *balanceRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := s.opController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	return allowed
}

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	opInfluence := s.opController.GetOpInfluence(cluster)
	s.OpController.GetFastOpInfluence(cluster, opInfluence)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	plan := newBalancePlan(kind, cluster, opInfluence)

	plan.source = s.PickSourceStore(cluster, plan)
	plan.region = s.PickRegion(cluster, plan)
	plan.target = s.PickTargetStore(cluster, plan)

	if op := s.ExecutePlan(plan); op != nil {
		return []*operator.Operator{op}
	}
	return nil
}

func (s *balanceRegionScheduler) PickSourceStore(cluster schedule.Cluster, plan *balancePlan) *core.StoreInfo {
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, s.filters, opts)

	sort.Slice(stores, func(i, j int) bool {
		iOp := plan.GetOpInfluence(stores[i].GetID())
		jOp := plan.GetOpInfluence(stores[j].GetID())
		return stores[i].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp) >
			stores[j].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp)
	})
	if len(stores) != 0 {
		return stores[0]
	}
	return nil
}

func (s *balanceRegionScheduler) PickTargetStore(cluster schedule.Cluster, plan *balancePlan) *core.StoreInfo {
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, plan.region.GetStoreIds()),
		filter.NewPlacementSafeguard(s.GetName(), plan.GetOpts(), plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source),
		filter.NewRegionScoreFilter(s.GetName(), plan.source, plan.GetOpts()),
		filter.NewSpecialUseFilter(s.GetName()),
		&filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}

	candidates := filter.NewCandidates(plan.GetStores()).
		FilterTarget(plan.GetOpts(), filters...).
		Sort(filter.RegionScoreComparer(plan.GetOpts()))

	if len(candidates.Stores) != 0 {
		return candidates.Stores[0]
	}
	return nil
}

func (s *balanceRegionScheduler) PickRegion(cluster schedule.Cluster, plan *balancePlan) *core.RegionInfo {
	var allowBalanceEmptyRegion func(*core.RegionInfo) bool

	switch cluster.(type) {
	case *schedule.RangeCluster:
		// allow empty region to be scheduled in range cluster
		allowBalanceEmptyRegion = func(region *core.RegionInfo) bool { return true }
	default:
		allowBalanceEmptyRegion = isAllowBalanceEmptyRegion(cluster)
	}
	// Priority pick the region that has a pending peer.
	// Pending region may means the disk is overload, remove the pending region firstly.
	region := cluster.RandPendingRegion(plan.SourceStoreID(), s.conf.Ranges, schedule.IsRegionHealthyAllowPending, schedule.ReplicatedRegion(cluster), allowBalanceEmptyRegion)
	if region == nil {
		// Then pick the region that has a follower in the source store.
		region = cluster.RandFollowerRegion(plan.SourceStoreID(), s.conf.Ranges, schedule.IsRegionHealthy, schedule.ReplicatedRegion(cluster), allowBalanceEmptyRegion)
	}
	if region == nil {
		// Then pick the region has the leader in the source store.
		region = cluster.RandLeaderRegion(plan.SourceStoreID(), s.conf.Ranges, schedule.IsRegionHealthy, schedule.ReplicatedRegion(cluster), allowBalanceEmptyRegion)
	}
	if region == nil {
		// Finally pick learner.
		region = cluster.RandLearnerRegion(plan.SourceStoreID(), s.conf.Ranges, schedule.IsRegionHealthy, schedule.ReplicatedRegion(cluster), allowBalanceEmptyRegion)
	}
	if region == nil {
		return nil
	}

	log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))

	// Skip hot regions.
	if cluster.IsRegionHot(plan.region) {
		log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
	}
	// Check region whether have leader
	if region.GetLeader() == nil {
		log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
	}
	return region
}

func (s *balanceRegionScheduler) ExecutePlan(plan *balancePlan) *operator.Operator {
	regionID := plan.region.GetID()
	sourceID := plan.source.GetID()
	targetID := plan.target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	if !plan.shouldBalance(s.GetName()) {
		return nil
	}

	oldPeer := plan.region.GetStorePeer(sourceID)
	newPeer := &metapb.Peer{StoreId: plan.target.GetID(), Role: oldPeer.Role}
	op, err := operator.CreateMovePeerOperator(BalanceRegionType, plan, plan.region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
		s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(plan.sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(plan.targetScore, 'f', 2, 64)
	return op
}

// isEmptyRegionAllowBalance checks if a region is an empty region and can be balanced.
func isEmptyRegionAllowBalance(cluster schedule.Cluster, region *core.RegionInfo) bool {
	return region.GetApproximateSize() > core.EmptyRegionApproximateSize || cluster.GetRegionCount() < core.InitClusterRegionThreshold
}

// isAllowBalanceEmptyRegion returns a function that checks if a region is an empty region and can be balanced.
func isAllowBalanceEmptyRegion(cluster schedule.Cluster) func(*core.RegionInfo) bool {
	return func(region *core.RegionInfo) bool { return isEmptyRegionAllowBalance(cluster, region) }
}
