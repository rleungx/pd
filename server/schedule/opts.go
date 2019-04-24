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
)

// Simulating is an option to overpass the impact of accelerated time. Should
// only turned on by the simulator.
var Simulating bool

// Options for schedulers.
type Options interface {
	GetLeaderScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetReplicaScheduleLimit() uint64
	GetMergeScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64

	GetMaxScheduleCost() int64
	GetStoreMaxScheduleCost() int64
	GetStoreBucketRate() float64
	OfflineStoreScheduleCost() int64
	OfflineStoreBucketRate() float64

	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxStoreDownTime() time.Duration
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetSplitMergeInterval() time.Duration

	GetMaxReplicas() int
	GetLocationLabels() []string

	GetHotRegionCacheHitsThreshold() int
	GetTolerantSizeRatio() float64
	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64

	IsRaftLearnerEnabled() bool

	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	IsNamespaceRelocationEnabled() bool

	CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool

	// test purpose
	GetTransferLeaderStepCost() int64
	GetAddPeerStepCost() int64
	GetRemovePeerStepCost() int64
	GetAddLearnerStepCost() int64
	GetPromoteLearnerStepCost() int64
	GetMergeRegionStepCost() int64
	GetMergeLeaderStepCost() int64
	GetSplitRegionStepCost() int64
	GetSplitLeaderStepCost() int64
}

// NamespaceOptions for namespace cluster.
type NamespaceOptions interface {
	GetLeaderScheduleLimit(name string) uint64
	GetRegionScheduleLimit(name string) uint64
	GetReplicaScheduleLimit(name string) uint64
	GetMergeScheduleLimit(name string) uint64
	GetMaxReplicas(name string) int
}

const (
	// RejectLeader is the label property type that sugguests a store should not
	// have any region leaders.
	RejectLeader = "reject-leader"
)
