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

package server

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/pingcap/errcode"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/region_syncer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	backgroundJobInterval = time.Minute
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	cachedCluster *clusterInfo

	coordinator *coordinator

	wg           sync.WaitGroup
	quit         chan struct{}
	regionSyncer *syncer.RegionSyncer
}

// ClusterStatus saves some state information
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
}

func newRaftCluster(s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		s:            s,
		running:      false,
		clusterID:    clusterID,
		clusterRoot:  s.getClusterRootPath(),
		regionSyncer: syncer.NewRegionSyncer(s),
	}
}

func (c *RaftCluster) loadClusterStatus() (*ClusterStatus, error) {
	data, err := c.s.kv.Load((c.s.kv.ClusterStatePath("raft_bootstrap_time")))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return &ClusterStatus{}, nil
	}
	t, err := parseTimestamp([]byte(data))
	if err != nil {
		return nil, err
	}
	return &ClusterStatus{RaftBootstrapTime: t}, nil
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	cluster, err := loadClusterInfo(c.s.idAlloc, c.s.kv, c.s.scheduleOpt)
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	err = c.s.classifier.ReloadNamespaces()
	if err != nil {
		return err
	}

	c.cachedCluster = cluster
	c.coordinator = newCoordinator(c.cachedCluster, c.s.hbStreams, c.s.classifier)
	c.cachedCluster.regionStats = newRegionStatistics(c.s.scheduleOpt, c.s.classifier)
	c.quit = make(chan struct{})

	c.wg.Add(3)
	go c.runCoordinator()
	go c.runBackgroundJobs(backgroundJobInterval)
	go c.syncRegions()
	c.running = true

	return nil
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer c.coordinator.wg.Wait()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator: Stopped coordinator")
}

func (c *RaftCluster) syncRegions() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.regionSyncer.RunServer(c.cachedCluster.changedRegionNotifier(), c.quit)
}

func (c *RaftCluster) stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return path.Join(clusterRootPath, "r", fmt.Sprintf("%020d", regionID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	peers := regionMeta.GetPeers()
	if len(peers) != 1 {
		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
	}

	peer := peers[0]
	if peer.GetStoreId() != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
	}
	if peer.GetId() == 0 {
		return errors.New("invalid zero peer id")
	}

	return nil
}

// GetRegionByKey gets region and leader peer by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.cachedCluster.searchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.cachedCluster.searchPrevRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.searchRegion(regionKey)
}

// ScanRegionsByKey scans region with start key, until number greater than limit.
func (c *RaftCluster) ScanRegionsByKey(startKey []byte, limit int) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.ScanRegions(startKey, limit)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	c.RLock()
	defer c.RUnlock()
	region := c.cachedCluster.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByID gets regionInfo by regionID from cluster.
func (c *RaftCluster) GetRegionInfoByID(regionID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegion(regionID)
}

// GetMetaRegions gets regions from cluster.
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getMetaRegions()
}

// GetRegions returns all regions info in detail.
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getRegions()
}

// GetStoreRegions returns all regions info with a given storeID.
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getStoreRegions(storeID)
}

// GetRegionStats returns region statistics from cluster.
func (c *RaftCluster) GetRegionStats(startKey, endKey []byte) *core.RegionStats {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getRegionStats(startKey, endKey)
}

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	c.cachedCluster.dropRegion(id)
}

// GetStores gets stores from cluster.
func (c *RaftCluster) GetStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getMetaStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) (*core.StoreInfo, error) {
	c.RLock()
	defer c.RUnlock()
	if storeID == 0 {
		return nil, errors.New("invalid zero store id")
	}

	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}
	return store, nil
}

// GetAdjacentRegions returns region's info that is adjacent with specific region id.
func (c *RaftCluster) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetAdjacentRegions(region)
}

// UpdateStoreLabels updates a store's location labels.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []*metapb.StoreLabel) error {
	c.RLock()
	defer c.RUnlock()
	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return errors.Errorf("invalid store ID %d, not found", storeID)
	}
	storeMeta := store.Store
	storeMeta.Labels = labels
	// putStore will perform label merge.
	err := c.putStore(storeMeta)
	return err
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.RLock()
	defer c.RUnlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	v, err := ParseVersion(store.GetVersion())
	if err != nil {
		return errors.Errorf("invalid put store %v, error: %s", store, err)
	}
	clusterVersion := c.cachedCluster.opt.loadClusterVersion()
	if !IsCompatible(clusterVersion, *v) {
		return errors.Errorf("version should compatible with version  %s, got %s", clusterVersion, v)
	}

	cluster := c.cachedCluster

	// Store address can not be the same as other stores.
	for _, s := range cluster.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetId() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.Store)
		}
	}

	s := cluster.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Update an existed store.
		s.Address = store.Address
		s.Version = store.Version
		s.MergeLabels(store.Labels)
	}
	// Check location labels.
	for _, k := range c.cachedCluster.GetLocationLabels() {
		if v := s.GetLabelValue(k); len(v) == 0 {
			log.Warnf("missing location label %q in store %v", k, s)
		}
	}
	return cluster.putStore(s)
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	op := errcode.Op("store.remove")
	c.RLock()
	defer c.RUnlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return op.AddTo(core.NewStoreNotFoundErr(storeID))
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return op.AddTo(core.StoreTombstonedErr{StoreID: storeID})
	}

	store.State = metapb.StoreState_Offline
	log.Warnf("[store %d] store %s has been Offline", store.GetId(), store.GetAddress())
	return cluster.putStore(store)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error { // revive:disable-line:flag-parameter
	c.RLock()
	defer c.RUnlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warnf("forcedly bury store %v", store)
	}

	store.State = metapb.StoreState_Tombstone
	log.Warnf("[store %d] store %s has been Tombstone", store.GetId(), store.GetAddress())
	return cluster.putStore(store)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.RLock()
	defer c.RUnlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	store.State = state
	log.Warnf("[store %d] set state to %v", storeID, state.String())
	return cluster.putStore(store)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leader, region float64) error {
	c.RLock()
	defer c.RUnlock()

	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	if err := c.s.kv.SaveStoreWeight(storeID, leader, region); err != nil {
		return err
	}

	store.LeaderWeight, store.RegionWeight = leader, region
	return c.cachedCluster.putStore(store)
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount uint64

	cluster := c.cachedCluster

	for _, store := range cluster.GetStores() {
		if store.GetState() != metapb.StoreState_Offline {
			if store.GetState() == metapb.StoreState_Up && !store.IsLowSpace(cluster.GetLowSpaceRatio()) {
				upStoreCount++
				continue
			}
		}
		offlineStore := store.Store
		// If the store is empty, it can be buried.
		if cluster.getStoreRegionCount(offlineStore.GetId()) == 0 {
			err := c.BuryStore(offlineStore.GetId(), false)
			if err != nil {
				log.Errorf("bury store %v failed: %v", offlineStore, err)
			} else {
				log.Infof("buried store %v", offlineStore)
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	if upStoreCount < c.s.GetConfig().Replication.MaxReplicas {
		for _, offlineStore := range offlineStores {
			log.Warnf("store %v may not turn into Tombstone, there are no extra up node has enough space to accommodate the extra replica", offlineStore)
		}
	}
}

func (c *RaftCluster) checkOperators() {
	oc := c.coordinator.opController
	for _, op := range oc.GetOperators() {
		// after region is merged, it will not heartbeat anymore
		// the operator of merged region will not timeout actively
		if c.cachedCluster.GetRegion(op.RegionID()) == nil {
			log.Debugf("remove operator %v cause region %d is merged", op, op.RegionID())
			oc.RemoveOperator(op)
			continue
		}

		if op.IsTimeout() {
			log.Infof("[region %v] operator timeout: %s", op.RegionID(), op)
			oc.RemoveOperator(op)
		}
	}
}

func (c *RaftCluster) collectMetrics() {
	cluster := c.cachedCluster
	statsMap := newStoreStatisticsMap(c.cachedCluster.opt, c.GetNamespaceClassifier())
	for _, s := range cluster.GetStores() {
		statsMap.Observe(s)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	cluster.collectMetrics()
	c.collectHealthStatus()
}

func (c *RaftCluster) collectHealthStatus() {
	client := c.s.GetClient()
	members, err := GetMembers(client)
	if err != nil {
		log.Info("get members error:", err)
	}
	unhealth := c.s.CheckHealth(members)
	for _, member := range members {
		if _, ok := unhealth[member.GetMemberId()]; ok {
			healthStatusGauge.WithLabelValues(member.GetName()).Set(0)
			continue
		}
		healthStatusGauge.WithLabelValues(member.GetName()).Set(1)
	}
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			c.checkOperators()
			c.checkStores()
			c.collectMetrics()
			c.coordinator.opController.PruneHistory()
		}
	}
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.getMeta()
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	c.RLock()
	defer c.RUnlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.cachedCluster.putMeta(meta)
}

// GetNamespaceClassifier returns current namespace classifier.
func (c *RaftCluster) GetNamespaceClassifier() namespace.Classifier {
	return c.s.classifier
}
