// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	collectTimeout            = 5 * time.Minute
	maxScheduleRetries        = 10

	regionheartbeatSendChanCap = 1024
	hotRegionScheduleName      = "balance-hot-region-scheduler"

	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.
)

var (
	errSchedulerExisted  = errors.New("scheduler existed")
	errSchedulerNotFound = errors.New("scheduler not found")
)

// coordinator is used to manage all schedulers and checkers to decide if the region needs to be scheduled.
type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster          *clusterInfo
	replicaChecker   *schedule.ReplicaChecker
	regionScatterer  *schedule.RegionScatterer
	namespaceChecker *schedule.NamespaceChecker
	mergeChecker     *schedule.MergeChecker
	schedulers       map[string]*scheduleController
	opController     *schedule.OperatorController
	classifier       namespace.Classifier
	hbStreams        *heartbeatStreams
}

// newCoordinator creates a new coordinator.
func newCoordinator(cluster *clusterInfo, hbStreams *heartbeatStreams, classifier namespace.Classifier) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:              ctx,
		cancel:           cancel,
		cluster:          cluster,
		replicaChecker:   schedule.NewReplicaChecker(cluster, classifier),
		regionScatterer:  schedule.NewRegionScatterer(cluster, classifier),
		namespaceChecker: schedule.NewNamespaceChecker(cluster, classifier),
		mergeChecker:     schedule.NewMergeChecker(cluster, classifier),
		schedulers:       make(map[string]*scheduleController),
		opController:     schedule.NewOperatorController(cluster, hbStreams),
		classifier:       classifier,
		hbStreams:        hbStreams,
	}
}

// patrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *coordinator) patrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetPatrolRegionInterval())
	defer timer.Stop()
	log.Info("coordinator: start patrol regions")
	start := time.Now()
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetPatrolRegionInterval())
		case <-c.ctx.Done():
			return
		}

		regions := c.cluster.ScanRegions(key, patrolScanRegionLimit)
		if len(regions) == 0 {
			// Resets the scan key.
			key = nil
			continue
		}

		for _, region := range regions {
			// Skip the region if there is already a pending operator.
			if c.opController.GetRunningOperator(region.GetID()) != nil || c.opController.GetWaitingOperator(region.GetID()) != nil {
				continue
			}

			key = region.GetEndKey()

			if c.checkRegion(region) {
				break
			}
		}
		// Updates the label level isolation statistics.
		c.cluster.updateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			patrolCheckRegionsHistogram.Observe(time.Since(start).Seconds())
			start = time.Now()
		}
	}
}

func (c *coordinator) checkRegion(region *core.RegionInfo) bool {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController
	for _, p := range region.GetLearners() {
		if region.GetPendingLearner(p.GetId()) != nil {
			continue
		}
		step := schedule.PromoteLearner{
			ToStore: p.GetStoreId(),
			PeerID:  p.GetId(),
		}
		op := schedule.NewOperator("promoteLearner", region.GetID(), region.GetRegionEpoch(), schedule.OpRegion, step)
		if opController.AddOperator(op) {
			return true
		}
	}

	if opController.OperatorCount(schedule.OpLeader) < c.cluster.GetLeaderScheduleLimit() &&
		opController.OperatorCount(schedule.OpRegion) < c.cluster.GetRegionScheduleLimit() &&
		opController.OperatorCount(schedule.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.namespaceChecker.Check(region); op != nil {
			if opController.AddOperator(op) {
				return true
			}
		}
	}

	if opController.OperatorCount(schedule.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.replicaChecker.Check(region); op != nil {
			if opController.AddOperator(op) {
				return true
			}
		}
	}

	if c.cluster.IsFeatureSupported(RegionMerge) && opController.OperatorCount(schedule.OpMerge) < c.cluster.GetMergeScheduleLimit() {
		if ops := c.mergeChecker.Check(region); ops != nil {
			// It makes sure that two operators can be added successfully altogether.
			if opController.AddOperator(ops...) {
				return true
			}
		}
	}
	return false
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator: Start collect cluster information")
	for {
		if c.shouldRun() {
			log.Info("coordinator: Cluster information is prepared")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			return
		}
	}
	log.Info("coordinator: Run scheduler")

	k := 0
	scheduleCfg := c.cluster.opt.load().clone()
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			log.Info("skip create ", schedulerCfg.Type)
			continue
		}
		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, schedulerCfg.Args...)
		if err != nil {
			log.Fatalf("can not create scheduler %s: %v", schedulerCfg.Type, err)
		}
		log.Infof("create scheduler %s", s.GetName())
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil {
			log.Errorf("can not add scheduler %s: %v", s.GetName(), err)
		}

		// Only records the valid scheduler config.
		if err == nil {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.opt.store(scheduleCfg)
	if err := c.cluster.opt.persist(c.cluster.kv); err != nil {
		log.Errorf("can't persist schedule config: %v", err)
	}

	c.wg.Add(1)
	// Starts to patrol regions.
	go c.patrolRegions()
}

func (c *coordinator) stop() {
	c.cancel()
}

// Hack to retrive info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotReadStatus() *core.StoreHotRegionInfos
	GetHotWriteStatus() *core.StoreHotRegionInfos
}

func (c *coordinator) getHotWriteRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotWriteStatus()
	}
	return nil
}

func (c *coordinator) getHotReadRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotReadStatus()
	}
	return nil
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()

	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if s.AllowSchedule() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	defer c.RUnlock()
	// Collects hot write region metrics.
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return
	}
	stores := c.cluster.GetStores()
	status := s.Scheduler.(hasHotStatus).GetHotWriteStatus()
	for _, s := range stores {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsPeer[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(0)
		}

		stat, ok = status.AsLeader[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(0)
		}
	}

	// Collects hot read region metrics.
	status = s.Scheduler.(hasHotStatus).GetHotReadStatus()
	for _, s := range stores {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsLeader[s.GetId()]
		if ok {
			totalReadBytes := float64(stat.TotalFlowBytes)
			hotReadRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(totalReadBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(hotReadRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(0)
		}
	}

}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	c.cluster.opt.AddSchedulerCfg(s.GetType(), args)

	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()

	s, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	s.Stop()
	schedulerStatusGauge.WithLabelValues(name, "allow").Set(0)
	delete(c.schedulers, name)

	return c.cluster.opt.RemoveSchedulerCfg(name)
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(); op != nil {
				c.opController.AddOperator(op...)
			}

		case <-s.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

// scheduleController is used to manage a scheduler to schedule.
type scheduleController struct {
	schedule.Scheduler
	cluster      *clusterInfo
	opController *schedule.OperatorController
	classifier   namespace.Classifier
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

// newScheduleController creates a new scheduleController.
func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		opController: c.opController,
		nextInterval: s.GetMinInterval(),
		classifier:   c.classifier,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) Schedule() []*schedule.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := scheduleByNamespace(s.cluster, s.classifier, s.Scheduler); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}
