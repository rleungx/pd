// Copyright 2019 TiKV Project Authors.
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
	"context"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/checker"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
)

// DefaultCacheSize is the default length of waiting list.
const DefaultCacheSize = 1000

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster           opt.Cluster
	opController      *OperatorController
	learnerChecker    *checker.LearnerChecker
	replicaChecker    *checker.ReplicaChecker
	ruleChecker       *checker.RuleChecker
	mergeChecker      *checker.MergeChecker
	regionWaitingList map[uint64]struct{}
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(ctx context.Context, cluster opt.Cluster, ruleManager *placement.RuleManager, opController *OperatorController) *CheckerController {
	return &CheckerController{
		cluster:           cluster,
		opController:      opController,
		learnerChecker:    checker.NewLearnerChecker(cluster),
		replicaChecker:    checker.NewReplicaChecker(cluster),
		ruleChecker:       checker.NewRuleChecker(cluster, ruleManager),
		mergeChecker:      checker.NewMergeChecker(ctx, cluster),
		regionWaitingList: make(map[uint64]struct{})
	}
}

// CheckRegion will check the region and add a new operator if needed.
func (c *CheckerController) CheckRegion(region *core.RegionInfo) (bool, []*operator.Operator) { //return checkerIsBusy,ops
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController
	checkerIsBusy := false
	if c.cluster.IsPlacementRulesEnabled() {
		if op := c.ruleChecker.Check(region); op != nil {
			if opController.OperatorCount(operator.OpReplica) >= c.cluster.GetReplicaScheduleLimit() {
				checkerIsBusy = true
				c.regionWaitingList[region.GetID()]= struct{}{}
				return checkerIsBusy, nil
			}
			return checkerIsBusy, []*operator.Operator{op}
		}
	} else {
		if op := c.learnerChecker.Check(region); op != nil {
			return false, []*operator.Operator{op}
		}
		if op := c.replicaChecker.Check(region); op != nil {
			if opController.OperatorCount(operator.OpReplica) >= c.cluster.GetReplicaScheduleLimit() {
				checkerIsBusy = true
				c.regionWaitingList[region.GetID()]= struct{}{}
				return checkerIsBusy, nil
			}
			return checkerIsBusy, []*operator.Operator{op}
		}
	}

	if ops := c.mergeChecker.Check(region); ops != nil {
		if c.mergeChecker != nil && opController.OperatorCount(operator.OpMerge) >= c.cluster.GetMergeScheduleLimit() {
			checkerIsBusy = true
			c.regionWaitingList[region.GetID()]= struct{}{}
			return checkerIsBusy, nil
		}
		return checkerIsBusy, ops
	}
	return checkerIsBusy, nil
}

// GetMergeChecker returns the merge checker.
func (c *CheckerController) GetMergeChecker() *checker.MergeChecker {
	return c.mergeChecker
}

// GetWaitingRegions returns the regions in the waiting list.
func (c *CheckerController) GetWaitingRegions() map[uint64]struct{} {
	return c.regionWaitingList
}

// RemoveWaitingRegion removes the region from the waiting list.
func (c *CheckerController) RemoveWaitingRegion(id uint64) {
	delete(c.regionWaitingList, id)
}
