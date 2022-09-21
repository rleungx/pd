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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

// Cluster State Statistics
//
// The target of cluster state statistics is to statistic the load state
// of a cluster given a time duration. The basic idea is to collect all
// the load information from every store at the same time duration and calculates
// the load for the whole cluster.
//
// Now we just support CPU as the measurement of the load. The CPU information
// is reported by each store with a heartbeat message which sending to PD every
// interval(10s). There is no synchronization between each store, so the stores
// could not send heartbeat messages at the same time, and the collected
// information has time shift.
//
// The diagram below demonstrates the time shift. "|" indicates the latest
// heartbeat.
//
// S1 ------------------------------|---------------------->
// S2 ---------------------------|------------------------->
// S3 ---------------------------------|------------------->
//
// The max time shift between 2 stores is 2*interval which is 20s here, and
// this is also the max time shift for the whole cluster. We assume that the
// time of starting to heartbeat is randomized, so the average time shift of
// the cluster is 10s. This is acceptable for statistics.
//
// Implementation
//
// Keep a 5min history statistics for each store, the history is stored in a
// circle array which evicting the oldest entry in a FIFO strategy. All the
// stores' histories combines into the cluster's history. So we can calculate
// any load value within 5 minutes. The algorithm for calculate is simple,
// Iterate each store's history from the latest entry with the same step and
// calculate the average CPU usage for the cluster.
//
// For example.
// To calculate the average load of the cluster within 3 minutes, start from the
// tail of circle array(which stores the history), and backward 18 steps to
// collect all the statistics that being accessed, then calculates the average
// CPU usage for this store. The average of all the stores CPU usage is the
// CPU usage of the whole cluster.
//

// LoadState indicates the load of a cluster or store
type LoadState int

// LoadStates that supported, None means no state determined
const (
	LoadStateNone LoadState = iota
	LoadStateLow
	LoadStateNormal
	LoadStateHigh
)

// String representation of LoadState
func (s LoadState) String() string {
	switch s {
	case LoadStateLow:
		return "low"
	case LoadStateNormal:
		return "normal"
	case LoadStateHigh:
		return "high"
	}
	return "none"
}

// NumberOfEntries is the max number of StatEntry that preserved,
// it is the history of a store's heartbeats. The interval of store
// heartbeats from TiKV is 10s, so we can preserve 30 entries per
// store which is about 5 minutes.
const NumberOfEntries = 30

// StatEntries saves the StatEntries for each store in the cluster
type StatEntries struct {
	stats map[uint64]*movingaverage.MaxFilter
	size  int   // size of entries to keep for each store
	total int64 // total of StatEntry appended
}

// NewStatEntries returns a statistics object for the cluster
func NewStatEntries(size int) *StatEntries {
	return &StatEntries{
		stats: make(map[uint64]*movingaverage.MaxFilter),
		size:  size,
	}
}

// Append an store StatEntry
func (se *StatEntries) Append(stores []*core.StoreInfo) {
	se.total++

	for _, store := range stores {
		storeID := store.GetID()
		if store.IsRemoved() {
			continue
		}
		// append the entry
		entries, ok := se.stats[storeID]
		if !ok {
			entries = movingaverage.NewMaxFilter(se.size)
			se.stats[storeID] = entries
		}

		entries.Add(float64(store.GetLoadScore()))
	}
}

// LoadStatus returns the load status of the cluster
func (se *StatEntries) LoadStatus(excludes ...uint64) float64 {
	// no entries have been collected
	if se.total == 0 || len(se.stats) == 0 {
		return 0
	}

	max := 0.0
	for storeID, stat := range se.stats {
		if slice.Contains(excludes, storeID) {
			continue
		}
		load := stat.Get()
		if load > max {
			max = load
		}
	}
	return max
}

// ClusterState ...
type ClusterState struct {
	se *StatEntries
}

func newClusterState() *ClusterState {
	return &ClusterState{
		se: NewStatEntries(NumberOfEntries),
	}
}

// GetState returns the state of the cluster, excludes is the list of store ID
// to be excluded
func (s *ClusterState) GetState(excludes ...uint64) LoadState {
	// Return LoadStateNone if there is not enough heartbeats
	// collected.
	if s.se.total < NumberOfEntries {
		return LoadStateNone
	}

	loadStatus := s.se.LoadStatus(excludes...)
	log.Debug("calculated load", zap.Float64("load-status", loadStatus))
	switch {
	case loadStatus > 0 && loadStatus < 30:
		return LoadStateLow
	case loadStatus >= 30 && loadStatus < 60:
		return LoadStateNormal
	case loadStatus >= 60:
		return LoadStateHigh
	}
	return LoadStateNone
}

// Collect statistics from store heartbeat
func (s *ClusterState) Collect(stores []*core.StoreInfo) {
	s.se.Append(stores)
}

// Reset ...
func (s *ClusterState) Reset() {
	s.se = NewStatEntries(NumberOfEntries)
}

// IsEmpty ...
func (s *ClusterState) IsEmpty() bool {
	return s.se.total == 0
}
