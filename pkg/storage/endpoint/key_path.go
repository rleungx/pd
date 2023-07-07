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

package endpoint

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/mcs/utils"
)

const (
<<<<<<< HEAD
	clusterPath                = "raft"
	configPath                 = "config"
	serviceMiddlewarePath      = "service_middleware"
	schedulePath               = "schedule"
	gcPath                     = "gc"
	rulesPath                  = "rules"
	ruleGroupPath              = "rule_group"
	regionLabelPath            = "region_label"
	replicationPath            = "replication_mode"
	customScheduleConfigPath   = "scheduler_config"
	gcWorkerServiceSafePointID = "gc_worker"
=======
	pdRootPath               = "/pd"
	clusterPath              = "raft"
	configPath               = "config"
	serviceMiddlewarePath    = "service_middleware"
	schedulePath             = "schedule"
	gcPath                   = "gc"
	rulesPath                = "rules"
	ruleGroupPath            = "rule_group"
	regionLabelPath          = "region_label"
	replicationPath          = "replication_mode"
	customScheduleConfigPath = "scheduler_config"
	// GCWorkerServiceSafePointID is the service id of GC worker.
	GCWorkerServiceSafePointID = "gc_worker"
>>>>>>> dbc936698... *: move keyspace group primary path code to key_path.go (#6755)
	minResolvedTS              = "min_resolved_ts"
	externalTimeStamp          = "external_timestamp"
	keyspaceSafePointPrefix    = "keyspaces/gc_safepoint"
	keyspaceGCSafePointSuffix  = "gc"
	keyspacePrefix             = "keyspaces"
	keyspaceMetaInfix          = "meta"
	keyspaceIDInfix            = "id"
	keyspaceAllocID            = "alloc_id"
	regionPathPrefix           = "raft/r"
	// resource group storage endpoint has prefix `resource_group`
	resourceGroupSettingsPath = "settings"
	resourceGroupStatesPath   = "states"
	controllerConfigPath      = "controller"
	// tso storage endpoint has prefix `tso`
	tsoServiceKey                = utils.TSOServiceName
	globalTSOAllocatorEtcdPrefix = "gta"
	// TimestampKey is the key of timestamp oracle used for the suffix.
	TimestampKey = "timestamp"

	tsoKeyspaceGroupPrefix      = tsoServiceKey + "/" + utils.KeyspaceGroupsKey
	keyspaceGroupsMembershipKey = "membership"
	keyspaceGroupsElectionKey   = "election"

	// we use uint64 to represent ID, the max length of uint64 is 20.
	keyLen = 20
)

// AppendToRootPath appends the given key to the rootPath.
func AppendToRootPath(rootPath string, key string) string {
	return path.Join(rootPath, key)
}

// ClusterRootPath appends the `clusterPath` to the rootPath.
func ClusterRootPath(rootPath string) string {
	return AppendToRootPath(rootPath, clusterPath)
}

// ClusterBootstrapTimeKey returns the path to save the cluster bootstrap timestamp.
func ClusterBootstrapTimeKey() string {
	return path.Join(clusterPath, "status", "raft_bootstrap_time")
}

func scheduleConfigPath(scheduleName string) string {
	return path.Join(customScheduleConfigPath, scheduleName)
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
	var buf strings.Builder
	buf.WriteString(regionPathPrefix)
	buf.WriteString("/")
	s := strconv.FormatUint(regionID, 10)
	if len(s) > keyLen {
		s = s[len(s)-keyLen:]
	} else {
		b := make([]byte, keyLen)
		diff := keyLen - len(s)
		for i := 0; i < keyLen; i++ {
			if i < diff {
				b[i] = 48
			} else {
				b[i] = s[i-diff]
			}
		}
		s = string(b)
	}
	buf.WriteString(s)

	return buf.String()
}

func resourceGroupSettingKeyPath(groupName string) string {
	return path.Join(resourceGroupSettingsPath, groupName)
}

func resourceGroupStateKeyPath(groupName string) string {
	return path.Join(resourceGroupStatesPath, groupName)
}

func ruleKeyPath(ruleKey string) string {
	return path.Join(rulesPath, ruleKey)
}

func ruleGroupIDPath(groupID string) string {
	return path.Join(ruleGroupPath, groupID)
}

func regionLabelKeyPath(ruleKey string) string {
	return path.Join(regionLabelPath, ruleKey)
}

func replicationModePath(mode string) string {
	return path.Join(replicationPath, mode)
}

func gcSafePointPath() string {
	return path.Join(gcPath, "safe_point")
}

// GCSafePointServicePrefixPath returns the GC safe point service key path prefix.
func GCSafePointServicePrefixPath() string {
	return path.Join(gcSafePointPath(), "service") + "/"
}

func gcSafePointServicePath(serviceID string) string {
	return path.Join(gcSafePointPath(), "service", serviceID)
}

// MinResolvedTSPath returns the min resolved ts path.
func MinResolvedTSPath() string {
	return path.Join(clusterPath, minResolvedTS)
}

// ExternalTimestampPath returns the external timestamp path.
func ExternalTimestampPath() string {
	return path.Join(clusterPath, externalTimeStamp)
}

// KeyspaceServiceSafePointPrefix returns the prefix of given service's service safe point.
// Prefix: /keyspaces/gc_safepoint/{space_id}/service/
func KeyspaceServiceSafePointPrefix(spaceID string) string {
	return path.Join(keyspaceSafePointPrefix, spaceID, "service") + "/"
}

// KeyspaceGCSafePointPath returns the gc safe point's path of the given key-space.
// Path: /keyspaces/gc_safepoint/{space_id}/gc
func KeyspaceGCSafePointPath(spaceID string) string {
	return path.Join(keyspaceSafePointPrefix, spaceID, keyspaceGCSafePointSuffix)
}

// KeyspaceServiceSafePointPath returns the path of given service's service safe point.
// Path: /keyspaces/gc_safepoint/{space_id}/service/{service_id}
func KeyspaceServiceSafePointPath(spaceID, serviceID string) string {
	return path.Join(KeyspaceServiceSafePointPrefix(spaceID), serviceID)
}

// KeyspaceSafePointPrefix returns prefix for all key-spaces' safe points.
// Path: /keyspaces/gc_safepoint/
func KeyspaceSafePointPrefix() string {
	return keyspaceSafePointPrefix + "/"
}

// KeyspaceGCSafePointSuffix returns the suffix for any gc safepoint.
// Postfix: /gc
func KeyspaceGCSafePointSuffix() string {
	return "/" + keyspaceGCSafePointSuffix
}

// KeyspaceMetaPrefix returns the prefix of keyspaces' metadata.
// Prefix: keyspaces/meta/
func KeyspaceMetaPrefix() string {
	return path.Join(keyspacePrefix, keyspaceMetaInfix) + "/"
}

// KeyspaceMetaPath returns the path to the given keyspace's metadata.
// Path: keyspaces/meta/{space_id}
func KeyspaceMetaPath(spaceID uint32) string {
	idStr := encodeKeyspaceID(spaceID)
	return path.Join(KeyspaceMetaPrefix(), idStr)
}

// KeyspaceIDPath returns the path to keyspace id from the given name.
// Path: keyspaces/id/{name}
func KeyspaceIDPath(name string) string {
	return path.Join(keyspacePrefix, keyspaceIDInfix, name)
}

// KeyspaceIDAlloc returns the path of the keyspace id's persistent window boundary.
// Path: keyspaces/alloc_id
func KeyspaceIDAlloc() string {
	return path.Join(keyspacePrefix, keyspaceAllocID)
}

// encodeKeyspaceID from uint32 to string.
// It adds extra padding to make encoded ID ordered.
// Encoded ID can be decoded directly with strconv.ParseUint.
// Width of the padded keyspaceID is 8 (decimal representation of uint24max is 16777215).
func encodeKeyspaceID(spaceID uint32) string {
	return fmt.Sprintf("%08d", spaceID)
}

// KeyspaceGroupIDPrefix returns the prefix of keyspace group id.
// Path: tso/keyspace_groups/membership
func KeyspaceGroupIDPrefix() string {
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupsMembershipKey)
}

// KeyspaceGroupIDPath returns the path to keyspace id from the given name.
// Path: tso/keyspace_groups/membership/{id}
func KeyspaceGroupIDPath(id uint32) string {
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupsMembershipKey, encodeKeyspaceGroupID(id))
}

// GetCompiledKeyspaceGroupIDRegexp returns the compiled regular expression for matching keyspace group id.
func GetCompiledKeyspaceGroupIDRegexp() *regexp.Regexp {
	pattern := strings.Join([]string{KeyspaceGroupIDPrefix(), `(\d{5})$`}, "/")
	return regexp.MustCompile(pattern)
}

// ResourceManagerSvcRootPath returns the root path of resource manager service.
// Path: /ms/{cluster_id}/resource_manager
func ResourceManagerSvcRootPath(clusterID uint64) string {
	return svcRootPath(clusterID, utils.ResourceManagerServiceName)
}

// TSOSvcRootPath returns the root path of tso service.
// Path: /ms/{cluster_id}/tso
func TSOSvcRootPath(clusterID uint64) string {
	return svcRootPath(clusterID, utils.TSOServiceName)
}

func svcRootPath(clusterID uint64, svcName string) string {
	c := strconv.FormatUint(clusterID, 10)
	return path.Join(utils.MicroserviceRootPath, c, svcName)
}

// LegacyRootPath returns the root path of legacy pd service.
// Path: /pd/{cluster_id}
func LegacyRootPath(clusterID uint64) string {
	return path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
}

// KeyspaceGroupPrimaryPath returns the path of keyspace group primary.
// default keyspace group: "/ms/{cluster_id}/tso/00000/primary".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}/primary".
func KeyspaceGroupPrimaryPath(rootPath string, keyspaceGroupID uint32) string {
	electionPath := KeyspaceGroupsElectionPath(rootPath, keyspaceGroupID)
	return path.Join(electionPath, utils.KeyspaceGroupsPrimaryKey)
}

// KeyspaceGroupsElectionPath returns the path of keyspace groups election.
// default keyspace group: "/ms/{cluster_id}/tso/00000".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}".
func KeyspaceGroupsElectionPath(rootPath string, keyspaceGroupID uint32) string {
	if keyspaceGroupID == utils.DefaultKeyspaceGroupID {
		return path.Join(rootPath, "00000")
	}
	return path.Join(rootPath, utils.KeyspaceGroupsKey, keyspaceGroupsElectionKey, fmt.Sprintf("%05d", keyspaceGroupID))
}

// GetCompiledNonDefaultIDRegexp returns the compiled regular expression for matching non-default keyspace group id.
func GetCompiledNonDefaultIDRegexp(clusterID uint64) *regexp.Regexp {
	rootPath := TSOSvcRootPath(clusterID)
	pattern := strings.Join([]string{rootPath, utils.KeyspaceGroupsKey, keyspaceGroupsElectionKey, `(\d{5})`, utils.KeyspaceGroupsPrimaryKey + `$`}, "/")
	return regexp.MustCompile(pattern)
}

// encodeKeyspaceGroupID from uint32 to string.
func encodeKeyspaceGroupID(groupID uint32) string {
	return fmt.Sprintf("%05d", groupID)
}

func buildPath(withSuffix bool, str ...string) string {
	var sb strings.Builder
	for i := 0; i < len(str); i++ {
		if i != 0 {
			sb.WriteString("/")
		}
		sb.WriteString(str[i])
	}
	if withSuffix {
		sb.WriteString("/")
	}
	return sb.String()
}

// KeyspaceGroupTSPath constructs the timestampOracle path prefix, which is:
//  1. for the default keyspace group:
//     "" in /pd/{cluster_id}/timestamp
//  2. for the non-default keyspace groups:
//     {group}/gta in /ms/{cluster_id}/tso/{group}/gta/timestamp
func KeyspaceGroupTSPath(groupID uint32) string {
	if groupID == utils.DefaultKeyspaceGroupID {
		return ""
	}
	return path.Join(fmt.Sprintf("%05d", groupID), globalTSOAllocatorEtcdPrefix)
}

// TimestampPath returns the timestamp path for the given timestamp oracle path prefix.
func TimestampPath(tsPath string) string {
	return path.Join(tsPath, TimestampKey)
}

// FullTimestampPath returns the full timestamp path.
//  1. for the default keyspace group:
//     /pd/{cluster_id}/timestamp
//  2. for the non-default keyspace groups:
//     /ms/{cluster_id}/tso/{group}/gta/timestamp
func FullTimestampPath(clusterID uint64, groupID uint32) string {
	rootPath := TSOSvcRootPath(clusterID)
	tsPath := TimestampPath(KeyspaceGroupTSPath(groupID))
	if groupID == utils.DefaultKeyspaceGroupID {
		rootPath = LegacyRootPath(clusterID)
	}
	return path.Join(rootPath, tsPath)
}
