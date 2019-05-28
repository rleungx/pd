// Copyright 2019 PingCAP, Inc.
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

package config_test

import (
	"encoding/json"
	"testing"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func (s *configTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *configTestSuite) TestConfig(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	scheduleCfg := server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(&scheduleCfg, DeepEquals, svr.GetScheduleConfig())

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	replicationCfg := server.ReplicationConfig{}
	c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
	c.Assert(&replicationCfg, DeepEquals, svr.GetReplicationConfig())

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion := semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(clusterVersion, Not(DeepEquals), svr.GetClusterVersion())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion = semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config show namespace <name> && config set namespace <type> <key> <value>
	args = []string{"-u", pdAddr, "table_ns", "create", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "table_ns", "set_store", "1", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "show", "namespace", "ts1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg := server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	args2 = []string{"-u", pdAddr, "config", "set", "namespace", "ts1", "region-schedule-limit", "128"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Not(Equals), svr.GetNamespaceConfig("ts1").RegionScheduleLimit)
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg = server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Equals, svr.GetNamespaceConfig("ts1").RegionScheduleLimit)

	// config delete namespace <name>
	args3 := []string{"-u", pdAddr, "config", "delete", "namespace", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg = server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Not(Equals), svr.GetNamespaceConfig("ts1").RegionScheduleLimit)

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg := server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config delete label-property <type> <key> <value>
	args3 = []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set <option> <value>
	args1 = []string{"-u", pdAddr, "config", "set", "leader-schedule-limit", "64"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-schedule-limit", "64"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "5"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	scheduleCfg = server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, svr.GetScheduleConfig().LeaderScheduleLimit)
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, svr.GetScheduleConfig().HotRegionScheduleLimit)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, svr.GetScheduleConfig().HotRegionCacheHitsThreshold)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, uint64(5))
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, uint64(64))
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, uint64(64))
	args1 = []string{"-u", pdAddr, "config", "set", "disable-raft-learner", "true"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	scheduleCfg = server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(scheduleCfg.DisableLearner, Equals, svr.GetScheduleConfig().DisableLearner)
}
