// Copyright 2023 TiKV Project Authors.
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

package keyspace_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	api "github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

// TestKeyspace need to prealloc keyspace IDs
// So we need another cluster to run this test.
func TestKeyspace(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := 1; i < 10; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestClusterWithKeyspaceGroup(ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
	keyspaceIDs, err := leaderServer.GetPreAllocKeyspaceIDs()
	re.NoError(err)

	var k api.KeyspaceMeta
	keyspaceName := "keyspace_1"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", "show", "name", keyspaceName}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return k.GetName() == keyspaceName
	})
	re.Equal(keyspaceIDs[0], k.GetId())
	re.Equal(defaultKeyspaceGroupID, k.Config[keyspace.TSOKeyspaceGroupIDKey])

	// split keyspace group.
	newGroupID := "2"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", newGroupID, strconv.Itoa(int(keyspaceIDs[0]))}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group in keyspace whether changed.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", "show", "name", keyspaceName}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return newGroupID == k.Config[keyspace.TSOKeyspaceGroupIDKey]
	})

	// test error name
	args := []string{"-u", pdAddr, "keyspace", "show", "name", "error_name"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

type keyspaceTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *pdTests.TestCluster
	pdAddr  string
}

func TestKeyspaceTestsuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	tc, err := pdTests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	suite.cluster = tc
	suite.pdAddr = tc.GetConfig().GetClientURL()
}

func (suite *keyspaceTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cancel()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func (suite *keyspaceTestSuite) TestShowKeyspace() {
	re := suite.Require()
	keyspaceName := constant.DefaultKeyspaceName
	keyspaceID := uint32(0)
	var k1, k2 api.KeyspaceMeta
	// Show by name.
	args := []string{"-u", suite.pdAddr, "keyspace", "show", "name", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &k1))
	re.Equal(keyspaceName, k1.GetName())
	re.Equal(keyspaceID, k1.GetId())
	// Show by ID.
	args = []string{"-u", suite.pdAddr, "keyspace", "show", "id", strconv.Itoa(int(keyspaceID))}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &k2))
	re.Equal(k1, k2)
}

func (suite *keyspaceTestSuite) TestCreateKeyspace() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name: "test_keyspace",
		Config: map[string]string{
			"foo":  "bar",
			"foo2": "bar2",
		},
	}
	meta := suite.mustCreateKeyspace(param)
	re.Equal(param.Name, meta.GetName())
	for k, v := range param.Config {
		re.Equal(v, meta.Config[k])
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name:   "test_keyspace",
		Config: map[string]string{"foo": "1"},
	}
	meta := suite.mustCreateKeyspace(param)
	re.Equal("1", meta.Config["foo"])

	// Update one existing config and add a new config, resulting in config: {foo: 2, foo2: 1}.
	args := []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=2,foo2=1"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal("test_keyspace", meta.GetName())
	re.Equal("2", meta.Config["foo"])
	re.Equal("1", meta.Config["foo2"])
	// Update one existing config and remove a config, resulting in config: {foo: 3}.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=3", "--remove", "foo2"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal("test_keyspace", meta.GetName())
	re.Equal("3", meta.Config["foo"])
	re.NotContains(meta.GetConfig(), "foo2")
	// Error if a key is specified in both --update and --remove list.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=4", "--remove", "foo"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
	// Error if a key is specified multiple times.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=4,foo=5"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name: "test_keyspace",
	}
	meta := suite.mustCreateKeyspace(param)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	// Disable the keyspace, capitalization shouldn't matter.
	args := []string{"-u", suite.pdAddr, "keyspace", "update-state", param.Name, "DiSAbleD"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal(keyspacepb.KeyspaceState_DISABLED, meta.State)
	// Tombstone the keyspace without archiving should fail.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-state", param.Name, "TOMBSTONE"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
}

func (suite *keyspaceTestSuite) TestListKeyspace() {
	re := suite.Require()
	var param api.CreateKeyspaceParams
	for i := range 10 {
		param = api.CreateKeyspaceParams{
			Name: fmt.Sprintf("test_keyspace_%d", i),
			Config: map[string]string{
				"foo": fmt.Sprintf("bar_%d", i),
			},
		}
		suite.mustCreateKeyspace(param)
	}
	// List all keyspaces, there should be 11 of them (default + 10 created above).
	args := []string{"-u", suite.pdAddr, "keyspace", "list"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	var resp api.LoadAllKeyspacesResponse
	re.NoError(json.Unmarshal(output, &resp))
	re.Len(resp.Keyspaces, 11)
	re.Empty(resp.NextPageToken) // No next page token since we load them all.
	re.Equal(constant.DefaultKeyspaceName, resp.Keyspaces[0].GetName())
	for i, meta := range resp.Keyspaces[1:] {
		re.Equal(fmt.Sprintf("test_keyspace_%d", i), meta.GetName())
		re.Equal(fmt.Sprintf("bar_%d", i), meta.Config["foo"])
	}
	// List 3 keyspaces staring with keyspace id 3, should results in keyspace id 3, 4, 5 and next page token 6.
	args = []string{"-u", suite.pdAddr, "keyspace", "list", "--limit", "3", "--page_token", "3"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &resp))
	re.Len(resp.Keyspaces, 3)
	for i, meta := range resp.Keyspaces {
		re.Equal(uint32(i+3), meta.GetId())
		re.Equal(fmt.Sprintf("test_keyspace_%d", i+2), meta.GetName())
		re.Equal(fmt.Sprintf("bar_%d", i+2), meta.Config["foo"])
	}
	re.Equal("6", resp.NextPageToken)
}

// Show command should auto retry without refresh_group_id if keyspace group manager not initialized.
// See issue: #7441
func (suite *keyspaceTestSuite) TestKeyspaceGroupUninitialized() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer().GetServer()
	kgm := leaderServer.GetKeyspaceGroupManager()
	leaderServer.SetKeyspaceGroupManager(nil)
	defer leaderServer.SetKeyspaceGroupManager(kgm)
	keyspaceName := constant.DefaultKeyspaceName
	keyspaceID := uint32(0)
	args := []string{"-u", suite.pdAddr, "keyspace", "show", "name", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	var meta api.KeyspaceMeta
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal(keyspaceName, meta.GetName())
	re.Equal(keyspaceID, meta.GetId())
}

func (suite *keyspaceTestSuite) TestSetPlacement() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "keyspace", Value: "test_label"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace"
	args := []string{"-u", suite.pdAddr, "keyspace", "create", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)

	var meta api.KeyspaceMeta
	err = json.Unmarshal(output, &meta)
	re.NoError(err)
	keyspaceID := meta.GetId()

	// Simulate region heartbeat for the keyspace range BEFORE setting placement rules
	region := suite.createKeyspaceRegion(keyspaceID)

	// Set store placement for the keyspace
	labelKey := "keyspace"
	labelValue := "test_label"
	args = []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), fmt.Sprintf("%s=%s", labelKey, labelValue)}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr := string(output)

	// The command should succeed
	re.Contains(outputStr, fmt.Sprintf("Successfully set placement rules for keyspace %d", keyspaceID))
	re.Contains(outputStr, fmt.Sprintf("%s=%s", labelKey, labelValue))

	// Verify the placement rule bundle was created
	groupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Equal(groupID, bundle.ID)
	re.Equal(100, bundle.Index)
	re.True(bundle.Override, "Override should be true to override default rules")

	// Verify rules
	rules := bundle.Rules
	re.Len(rules, 2, "should have 2 rules (raw and txn)")

	// Check first rule (raw key space)
	rule1 := rules[0]
	re.Equal(groupID, rule1.GroupID)
	re.Equal(fmt.Sprintf("keyspace-%d-rule", keyspaceID), rule1.ID)
	re.Equal("voter", string(rule1.Role))
	re.Equal(3, rule1.Count)
	// Verify StartKey and EndKey match the keyspace boundaries
	regionBound := keyspace.MakeRegionBound(keyspaceID)
	expectedStartKey := hex.EncodeToString(regionBound.RawLeftBound)
	expectedEndKey := hex.EncodeToString(regionBound.RawRightBound)
	actualStartKey := hex.EncodeToString(rule1.StartKey)
	actualEndKey := hex.EncodeToString(rule1.EndKey)
	re.Equal(expectedStartKey, actualStartKey, "raw rule StartKey should match keyspace boundary")
	re.Equal(expectedEndKey, actualEndKey, "raw rule EndKey should match keyspace boundary")

	// Verify label constraints
	re.Len(rule1.LabelConstraints, 1)
	re.Equal(labelKey, rule1.LabelConstraints[0].Key)
	re.Equal("in", string(rule1.LabelConstraints[0].Op))
	re.Equal([]string{labelValue}, rule1.LabelConstraints[0].Values)

	// Check second rule (txn key space)
	rule2 := rules[1]
	re.Equal(groupID, rule2.GroupID)
	re.Equal(fmt.Sprintf("keyspace-%d-rule-txn", keyspaceID), rule2.ID)
	re.Equal("voter", string(rule2.Role))
	re.Equal(3, rule2.Count)
	// Verify txn key space StartKey and EndKey
	expectedTxnStartKey := hex.EncodeToString(regionBound.TxnLeftBound)
	expectedTxnEndKey := hex.EncodeToString(regionBound.TxnRightBound)
	actualTxnStartKey := hex.EncodeToString(rule2.StartKey)
	actualTxnEndKey := hex.EncodeToString(rule2.EndKey)
	re.Equal(expectedTxnStartKey, actualTxnStartKey, "txn rule StartKey should match keyspace boundary")
	re.Equal(expectedTxnEndKey, actualTxnEndKey, "txn rule EndKey should match keyspace boundary")

	// Verify label constraints
	re.Len(rule2.LabelConstraints, 1)
	re.Equal("keyspace", rule2.LabelConstraints[0].Key)
	re.Equal("in", string(rule2.LabelConstraints[0].Op))
	re.Equal([]string{labelValue}, rule2.LabelConstraints[0].Values)

	// Verify rules are applied to the region we created earlier
	re.NotNil(region, "region should exist")

	// Check region placement using the handler
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(region)
	re.NoError(err)
	re.NotNil(regionFit, "region fit should exist")

	// Verify that only our custom rule is applied (Override=true should replace default rules)
	re.Len(regionFit.RuleFits, 1, "should only have one rule fit due to Override=true")
	appliedRule := regionFit.RuleFits[0]
	re.Equal(groupID, appliedRule.Rule.GroupID, "applied rule should be from our custom keyspace group")
	re.Equal(rule2.ID, appliedRule.Rule.ID, "applied rule should be the txn rule")
	re.Len(appliedRule.Rule.LabelConstraints, 1)
	re.Equal(labelKey, appliedRule.Rule.LabelConstraints[0].Key)
	re.Equal(labelValue, appliedRule.Rule.LabelConstraints[0].Values[0])
}

func (suite *keyspaceTestSuite) TestRevertPlacement() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "keyspace", Value: "test_label"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace_revert"
	args := []string{"-u", suite.pdAddr, "keyspace", "create", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)

	var meta api.KeyspaceMeta
	err = json.Unmarshal(output, &meta)
	re.NoError(err)
	keyspaceID := meta.GetId()

	// Simulate region heartbeat for the keyspace range BEFORE setting placement rules
	region := suite.createKeyspaceRegion(keyspaceID)

	// Set store placement for the keyspace
	labelKey := "keyspace"
	labelValue := "test_label"
	args = []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), fmt.Sprintf("%s=%s", labelKey, labelValue)}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr := string(output)

	// Verify the command output
	re.Contains(outputStr, fmt.Sprintf("Successfully set placement rules for keyspace %d", keyspaceID))
	re.Contains(outputStr, fmt.Sprintf("%s=%s", labelKey, labelValue))

	// Verify the bundle was created
	groupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Equal(groupID, bundle.ID)
	re.Len(bundle.Rules, 2)
	// Verify the label constraints
	re.Equal(labelKey, bundle.Rules[0].LabelConstraints[0].Key)
	re.Equal(labelValue, bundle.Rules[0].LabelConstraints[0].Values[0])

	// Verify rules are applied to the region we created earlier
	re.NotNil(region, "region should exist")
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(region)
	re.NoError(err)
	re.NotNil(regionFit)
	// Verify that only our custom rule is applied (Override=true should replace default rules)
	re.Len(regionFit.RuleFits, 1, "should only have one rule fit due to Override=true")
	re.Equal(groupID, regionFit.RuleFits[0].Rule.GroupID, "applied rule should be from our custom keyspace group")

	// Revert the placement rules
	args = []string{"-u", suite.pdAddr, "keyspace", "revert-placement", strconv.Itoa(int(keyspaceID))}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr = string(output)

	// The command should succeed
	re.Contains(outputStr, fmt.Sprintf("Successfully reverted placement rules for keyspace %d", keyspaceID))

	// Verify the bundle was deleted - check that the group no longer exists
	group := leaderServer.GetRaftCluster().GetRuleManager().GetRuleGroup(groupID)
	re.Nil(group, "rule group should be deleted")
	// Also verify that there are no rules for this group
	bundle = leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Empty(bundle.Rules, "bundle rules should be empty")

	// Verify rules are no longer applied to regions after revert
	// The region should fall back to default rules
	regionFitAfter, err := handler.CheckRegionPlacementRule(region)
	re.NoError(err)
	// After revert, should use only one default placement rule (not the keyspace-specific rules)
	re.Len(regionFitAfter.RuleFits, 1, "should have only one default rule applied after revert")
	re.NotEqual(groupID, regionFitAfter.RuleFits[0].Rule.GroupID, "should use default rule, not keyspace-specific rule")
}

// TestSetPlacementWithTiFlash ensures keyspace set-placement coexists with TiFlash rules.
// It verifies that:
// 1. set-placement does not remove or alter an existing TiFlash learner bundle
// 2. Keyspace voter rules in keyspace-{id} remain intact
// 3. A TiFlash region within the keyspace matches both voter and learner rules concurrently
func (suite *keyspaceTestSuite) TestSetPlacementWithTiFlash() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()
	ctx := context.Background()

	httpClient := pd.NewClient("pd-keyspace-test", []string{suite.pdAddr})
	defer httpClient.Close()

	// Add TiKV stores
	for i := 1; i <= 3; i++ {
		pdTests.MustPutStore(re, suite.cluster, &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels:  []*metapb.StoreLabel{{Key: "keyspace", Value: "test_label"}},
			State:   metapb.StoreState_Up,
		})
	}

	// Add TiFlash stores
	for i := 4; i <= 6; i++ {
		pdTests.MustPutStore(re, suite.cluster, &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tiflash-%d:%d", i, i),
			Labels:  []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
			State:   metapb.StoreState_Up,
		})
	}

	// Create keyspace and prepare TiFlash rule range inside its txn range
	meta := suite.mustCreateKeyspace(api.CreateKeyspaceParams{Name: "tiflash_test"})
	keyspaceID := meta.GetId()
	bound := keyspace.MakeRegionBound(keyspaceID)
	regionStartKey := append([]byte(nil), bound.TxnLeftBound...)
	regionEndKey := append([]byte(nil), bound.TxnRightBound...)
	regionStartKeyHex := hex.EncodeToString(regionStartKey)
	regionEndKeyHex := hex.EncodeToString(regionEndKey)

	// Create TiFlash rule before running set-placement to ensure it is preserved
	tiflashGroupID := fmt.Sprintf("tiflash-table-%d", keyspaceID)
	tiflashBundle := &pd.GroupBundle{
		ID:       tiflashGroupID,
		Index:    200,
		Override: false,
		Rules: []*pd.Rule{{
			GroupID:     tiflashGroupID,
			ID:          "tiflash-learner",
			Role:        pd.Learner,
			Count:       1,
			StartKeyHex: regionStartKeyHex,
			EndKeyHex:   regionEndKeyHex,
			LabelConstraints: []pd.LabelConstraint{{
				Key: "engine", Op: pd.In, Values: []string{"tiflash"},
			}},
		}},
	}
	re.NoError(httpClient.SetPlacementRuleBundles(ctx, []*pd.GroupBundle{tiflashBundle}, true))

	// Run set-placement; it should not override the TiFlash rule bundle
	args := []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "keyspace=test_label"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Successfully set placement rules")

	// Verify keyspace bundle exists and targets TiKV stores
	groupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Len(bundle.Rules, 2, "keyspace should have 2 voter rules (raw + txn)")
	re.True(bundle.Override)
	for _, rule := range bundle.Rules {
		re.Equal(placement.Voter, rule.Role, "keyspace rules should be voter role")
		re.Len(rule.LabelConstraints, 1)
		re.Equal("keyspace", rule.LabelConstraints[0].Key)
		re.Contains(rule.LabelConstraints[0].Values, "test_label")
	}

	// TiFlash bundle should still exist and remain untouched
	tiflashBundleVerify := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(tiflashGroupID)
	re.NotNil(tiflashBundleVerify, "TiFlash bundle should exist after set-placement")
	re.Len(tiflashBundleVerify.Rules, 1, "TiFlash bundle should keep its learner rule")
	re.Equal(placement.Learner, tiflashBundleVerify.Rules[0].Role, "TiFlash rule should be learner role")
	re.Len(tiflashBundleVerify.Rules[0].LabelConstraints, 1)
	re.Equal("engine", tiflashBundleVerify.Rules[0].LabelConstraints[0].Key)
	re.Contains(tiflashBundleVerify.Rules[0].LabelConstraints[0].Values, "tiflash")
	re.Equal(regionStartKey, tiflashBundleVerify.Rules[0].StartKey, "TiFlash rule start key should remain unchanged")
	re.Equal(regionEndKey, tiflashBundleVerify.Rules[0].EndKey, "TiFlash rule end key should remain unchanged")
	re.False(tiflashBundleVerify.Override, "TiFlash bundle should have Override=false to coexist with keyspace rules")

	// Both bundles should be registered in the rule manager
	allBundles := leaderServer.GetRaftCluster().GetRuleManager().GetAllGroupBundles()
	bundleIDs := make([]string, 0, len(allBundles))
	for _, b := range allBundles {
		bundleIDs = append(bundleIDs, b.ID)
	}
	re.Contains(bundleIDs, groupID, "keyspace bundle should exist")
	re.Contains(bundleIDs, tiflashGroupID, "TiFlash bundle should coexist with keyspace bundle")

	// Create a region within the keyspace range that has both TiKV voters and TiFlash learner
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
		{Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       uint64(1000),
			StartKey: append([]byte(nil), regionStartKey...),
			EndKey:   append([]byte(nil), regionEndKey...),
			Peers:    peers,
		},
		peers[0],
	)
	leaderServer.GetRaftCluster().HandleRegionHeartbeat(region)

	// Wait for bundles to apply and ensure both rules match the region
	testutil.Eventually(re, func() bool {
		handler := leaderServer.GetServer().GetHandler()
		regionFit, err := handler.CheckRegionPlacementRule(region)
		if err != nil || regionFit == nil {
			return false
		}
		hasKeyspaceVoterRule := false
		hasTiFlashLearnerRule := false
		for _, fit := range regionFit.RuleFits {
			if fit.Rule.GroupID == groupID && fit.Rule.Role == placement.Voter {
				hasKeyspaceVoterRule = true
			}
			if fit.Rule.GroupID == tiflashGroupID && fit.Rule.Role == placement.Learner {
				hasTiFlashLearnerRule = true
			}
		}
		return hasKeyspaceVoterRule && hasTiFlashLearnerRule
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	// Final verification on rule fits
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(region)
	re.NoError(err)
	re.NotNil(regionFit, "region fit should exist")
	re.GreaterOrEqual(len(regionFit.RuleFits), 2, "region should match at least keyspace and TiFlash rules")

	hasKeyspaceVoterRule := false
	for _, fit := range regionFit.RuleFits {
		if fit.Rule.GroupID == groupID && fit.Rule.Role == placement.Voter {
			hasKeyspaceVoterRule = true
			re.Len(fit.Rule.LabelConstraints, 1)
			re.Equal("keyspace", fit.Rule.LabelConstraints[0].Key)
			re.Contains(fit.Rule.LabelConstraints[0].Values, "test_label")
			break
		}
	}
	re.True(hasKeyspaceVoterRule, "region should match keyspace voter rule")

	hasTiFlashLearnerRule := false
	for _, fit := range regionFit.RuleFits {
		if fit.Rule.GroupID == tiflashGroupID && fit.Rule.Role == placement.Learner {
			hasTiFlashLearnerRule = true
			re.Len(fit.Rule.LabelConstraints, 1)
			re.Equal("engine", fit.Rule.LabelConstraints[0].Key)
			re.Contains(fit.Rule.LabelConstraints[0].Values, "tiflash")
			break
		}
	}
	re.True(hasTiFlashLearnerRule, "region should match TiFlash learner rule")
}

func (suite *keyspaceTestSuite) TestSetPlacementMultipleLabels() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with multiple labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "zone", Value: "east"},
				{Key: "disk", Value: "ssd"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace"
	args := []string{"-u", suite.pdAddr, "keyspace", "create", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)

	var meta api.KeyspaceMeta
	err = json.Unmarshal(output, &meta)
	re.NoError(err)
	keyspaceID := meta.GetId()

	// Simulate region heartbeat for the keyspace range BEFORE setting placement rules
	region := suite.createKeyspaceRegion(keyspaceID)

	// Set placement with multiple label constraints
	args = []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "zone=east", "disk=ssd"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr := string(output)

	// The command should succeed and show both constraints
	re.Contains(outputStr, fmt.Sprintf("Successfully set placement rules for keyspace %d", keyspaceID))
	re.Contains(outputStr, "zone=east")
	re.Contains(outputStr, "disk=ssd")

	// Verify the placement rule bundle was created with multiple constraints
	groupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Equal(groupID, bundle.ID)
	re.True(bundle.Override, "Override should be true to override default rules")
	re.Len(bundle.Rules, 2, "should have 2 rules (raw and txn)")

	// Check both rules have both label constraints
	for _, rule := range bundle.Rules {
		re.Len(rule.LabelConstraints, 2, "each rule should have 2 label constraints")

		// Verify first constraint (zone=east)
		re.Equal("zone", rule.LabelConstraints[0].Key)
		re.Equal("in", string(rule.LabelConstraints[0].Op))
		re.Equal([]string{"east"}, rule.LabelConstraints[0].Values)

		// Verify second constraint (disk=ssd)
		re.Equal("disk", rule.LabelConstraints[1].Key)
		re.Equal("in", string(rule.LabelConstraints[1].Op))
		re.Equal([]string{"ssd"}, rule.LabelConstraints[1].Values)
	}

	// Verify rules are applied to the region we created earlier
	re.NotNil(region, "region should exist")
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(region)
	re.NoError(err)
	re.NotNil(regionFit)

	// Verify that only our custom rule is applied (Override=true should replace default rules)
	re.Len(regionFit.RuleFits, 1, "should only have one rule fit due to Override=true")
	appliedRule := regionFit.RuleFits[0]
	re.Equal(groupID, appliedRule.Rule.GroupID, "applied rule should be from our custom keyspace group")
	re.Len(appliedRule.Rule.LabelConstraints, 2, "rule should have 2 constraints")
	re.Equal("zone", appliedRule.Rule.LabelConstraints[0].Key)
	re.Equal([]string{"east"}, appliedRule.Rule.LabelConstraints[0].Values)
	re.Equal("disk", appliedRule.Rule.LabelConstraints[1].Key)
	re.Equal([]string{"ssd"}, appliedRule.Rule.LabelConstraints[1].Values)
}

// createKeyspaceRegion creates a region in the keyspace range by simulating heartbeat.
// It returns the created region for verification.
func (suite *keyspaceTestSuite) createKeyspaceRegion(keyspaceID uint32) *core.RegionInfo {
	re := suite.Require()
	regionID := uint64(keyspaceID)

	// Use keyspace.MakeRegionBound to get the correct region boundaries
	regionBound := keyspace.MakeRegionBound(keyspaceID)

	return pdTests.MustPutRegion(re, suite.cluster, regionID, 2, regionBound.TxnLeftBound, regionBound.TxnRightBound)
}

// createTableRegion creates a region for a specific table within a keyspace.
// It returns the created region for verification.
func (suite *keyspaceTestSuite) createTableRegion(keyspaceID uint32, tableID int64) *core.RegionInfo {
	re := suite.Require()
	regionID := uint64(keyspaceID)*1000 + uint64(tableID)

	// Generate table key range within the keyspace
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)
	keyspaceTxnPrefix := append([]byte{'x'}, keyspaceIDBytes[1:]...)

	tableStartKey := codec.GenerateTableKey(tableID)
	tableEndKey := codec.GenerateTableKey(tableID + 1)

	txnStartKeyBytes := append(keyspaceTxnPrefix, tableStartKey...)
	txnEndKeyBytes := append(keyspaceTxnPrefix, tableEndKey...)

	txnStartKey := codec.EncodeBytes(txnStartKeyBytes)
	txnEndKey := codec.EncodeBytes(txnEndKeyBytes)

	return pdTests.MustPutRegion(re, suite.cluster, regionID, 2, txnStartKey, txnEndKey)
}

func (suite *keyspaceTestSuite) mustCreateKeyspace(param api.CreateKeyspaceParams) api.KeyspaceMeta {
	re := suite.Require()
	var meta api.KeyspaceMeta
	args := []string{"-u", suite.pdAddr, "keyspace", "create", param.Name}
	for k, v := range param.Config {
		args = append(args, "--config", fmt.Sprintf("%s=%s", k, v))
	}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	return meta
}

func (suite *keyspaceTestSuite) TestShowKeyspaceRange() {
	re := suite.Require()
	// Use the default keyspace which always exists
	keyspaceID := uint32(0)
	keyspaceName := constant.DefaultKeyspaceName

	// Test range by ID with default (txn) mode
	args := []string{"-u", suite.pdAddr, "keyspace", "range", "id", strconv.Itoa(int(keyspaceID))}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	var ranges map[string]string
	re.NoError(json.Unmarshal(output, &ranges))
	re.Len(ranges, 2)

	// Verify txn key range
	bound := keyspace.MakeRegionBound(keyspaceID)
	expectedStartKey := hex.EncodeToString(bound.TxnLeftBound)
	expectedEndKey := hex.EncodeToString(bound.TxnRightBound)
	re.Equal(expectedStartKey, ranges["start_key"])
	re.Equal(expectedEndKey, ranges["end_key"])

	// Test range by ID with raw mode
	args = []string{"-u", suite.pdAddr, "keyspace", "range", "id", strconv.Itoa(int(keyspaceID)), "--raw"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &ranges))
	re.Len(ranges, 2)

	// Verify raw key range
	expectedStartKeyRaw := hex.EncodeToString(bound.RawLeftBound)
	expectedEndKeyRaw := hex.EncodeToString(bound.RawRightBound)
	re.Equal(expectedStartKeyRaw, ranges["start_key"])
	re.Equal(expectedEndKeyRaw, ranges["end_key"])

	// Test range by name with default (txn) mode
	args = []string{"-u", suite.pdAddr, "keyspace", "range", "name", keyspaceName}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &ranges))
	re.Len(ranges, 2)
	re.Equal(expectedStartKey, ranges["start_key"])
	re.Equal(expectedEndKey, ranges["end_key"])

	// Test range by name with raw mode
	args = []string{"-u", suite.pdAddr, "keyspace", "range", "name", keyspaceName, "--raw"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &ranges))
	re.Len(ranges, 2)
	re.Equal(expectedStartKeyRaw, ranges["start_key"])
	re.Equal(expectedEndKeyRaw, ranges["end_key"])

	// Test error case: invalid keyspace ID
	args = []string{"-u", suite.pdAddr, "keyspace", "range", "id", "invalid_id"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "should be a valid number")

	// Test error case: non-existent keyspace name
	args = []string{"-u", suite.pdAddr, "keyspace", "range", "name", "non_existent_keyspace"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
}

func (suite *keyspaceTestSuite) TestSetPlacementTableLevel() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "zone", Value: "east"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace_table"
	meta := suite.mustCreateKeyspace(api.CreateKeyspaceParams{Name: keyspaceName})
	keyspaceID := meta.GetId()

	// Set table-level placement rules
	tableID := int64(100)
	labelKey := "zone"
	labelValue := "east"
	args := []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "--table-id", strconv.FormatInt(tableID, 10), fmt.Sprintf("%s=%s", labelKey, labelValue)}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr := string(output)

	// The command should succeed
	re.Contains(outputStr, fmt.Sprintf("Successfully set placement rules for keyspace %d table %d", keyspaceID, tableID))
	re.Contains(outputStr, fmt.Sprintf("%s=%s", labelKey, labelValue))

	// Verify the placement rule bundle was created with table-level group ID
	groupID := fmt.Sprintf("keyspace-%d-table-%d", keyspaceID, tableID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Equal(groupID, bundle.ID)
	re.Equal(100, bundle.Index)
	re.True(bundle.Override, "Override should be true to override default rules")

	// Verify rules
	rules := bundle.Rules
	re.Len(rules, 2, "should have 2 rules (raw and txn)")

	// Check first rule (raw key space)
	rule1 := rules[0]
	re.Equal(groupID, rule1.GroupID)
	re.Equal(fmt.Sprintf("keyspace-%d-table-%d-rule", keyspaceID, tableID), rule1.ID)
	re.Equal("voter", string(rule1.Role))
	re.Equal(3, rule1.Count)

	// Verify StartKey and EndKey match the table key ranges
	// Generate expected table key ranges
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)
	keyspaceRawPrefix := append([]byte{'r'}, keyspaceIDBytes[1:]...)
	keyspaceTxnPrefix := append([]byte{'x'}, keyspaceIDBytes[1:]...)

	tableStartKey := codec.GenerateTableKey(tableID)
	tableEndKey := codec.GenerateTableKey(tableID + 1)

	rawStartKeyBytes := append(keyspaceRawPrefix, tableStartKey...)
	rawEndKeyBytes := append(keyspaceRawPrefix, tableEndKey...)
	txnStartKeyBytes := append(keyspaceTxnPrefix, tableStartKey...)
	txnEndKeyBytes := append(keyspaceTxnPrefix, tableEndKey...)

	expectedRawStartKey := hex.EncodeToString(codec.EncodeBytes(rawStartKeyBytes))
	expectedRawEndKey := hex.EncodeToString(codec.EncodeBytes(rawEndKeyBytes))
	expectedTxnStartKey := hex.EncodeToString(codec.EncodeBytes(txnStartKeyBytes))
	expectedTxnEndKey := hex.EncodeToString(codec.EncodeBytes(txnEndKeyBytes))

	actualRawStartKey := hex.EncodeToString(rule1.StartKey)
	actualRawEndKey := hex.EncodeToString(rule1.EndKey)
	re.Equal(expectedRawStartKey, actualRawStartKey, "raw rule StartKey should match table key range")
	re.Equal(expectedRawEndKey, actualRawEndKey, "raw rule EndKey should match table key range")

	// Verify label constraints
	re.Len(rule1.LabelConstraints, 1)
	re.Equal(labelKey, rule1.LabelConstraints[0].Key)
	re.Equal("in", string(rule1.LabelConstraints[0].Op))
	re.Equal([]string{labelValue}, rule1.LabelConstraints[0].Values)

	// Check second rule (txn key space)
	rule2 := rules[1]
	re.Equal(groupID, rule2.GroupID)
	re.Equal(fmt.Sprintf("keyspace-%d-table-%d-rule-txn", keyspaceID, tableID), rule2.ID)
	re.Equal("voter", string(rule2.Role))
	re.Equal(3, rule2.Count)

	// Verify txn key space StartKey and EndKey
	actualTxnStartKey := hex.EncodeToString(rule2.StartKey)
	actualTxnEndKey := hex.EncodeToString(rule2.EndKey)
	re.Equal(expectedTxnStartKey, actualTxnStartKey, "txn rule StartKey should match table key range")
	re.Equal(expectedTxnEndKey, actualTxnEndKey, "txn rule EndKey should match table key range")

	// Verify label constraints
	re.Len(rule2.LabelConstraints, 1)
	re.Equal(labelKey, rule2.LabelConstraints[0].Key)
	re.Equal("in", string(rule2.LabelConstraints[0].Op))
	re.Equal([]string{labelValue}, rule2.LabelConstraints[0].Values)

	// Verify that keyspace-level bundle does not exist (only table-level exists)
	keyspaceGroupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	keyspaceBundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(keyspaceGroupID)
	re.Empty(keyspaceBundle.Rules, "keyspace-level bundle should not exist")

	// Create a region for the table and verify rules are applied
	tableRegion := suite.createTableRegion(keyspaceID, tableID)
	re.NotNil(tableRegion, "table region should exist")

	// Check region placement using the handler
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(tableRegion)
	re.NoError(err)
	re.NotNil(regionFit, "region fit should exist")

	// Verify that the table-level rule is applied
	re.Len(regionFit.RuleFits, 1, "should only have one rule fit due to Override=true")
	appliedRule := regionFit.RuleFits[0]
	re.Equal(groupID, appliedRule.Rule.GroupID, "applied rule should be from our custom table group")
	re.Equal(rule2.ID, appliedRule.Rule.ID, "applied rule should be the txn rule")
	re.Len(appliedRule.Rule.LabelConstraints, 1)
	re.Equal(labelKey, appliedRule.Rule.LabelConstraints[0].Key)
	re.Equal(labelValue, appliedRule.Rule.LabelConstraints[0].Values[0])
}

func (suite *keyspaceTestSuite) TestRevertPlacementTableLevel() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "zone", Value: "east"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace_revert_table"
	meta := suite.mustCreateKeyspace(api.CreateKeyspaceParams{Name: keyspaceName})
	keyspaceID := meta.GetId()

	// Create a table region before setting placement rules
	tableID := int64(200)
	tableRegion := suite.createTableRegion(keyspaceID, tableID)
	re.NotNil(tableRegion, "table region should exist")

	// Set table-level placement rules
	labelKey := "zone"
	labelValue := "east"
	args := []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "--table-id", strconv.FormatInt(tableID, 10), fmt.Sprintf("%s=%s", labelKey, labelValue)}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr := string(output)

	// Verify the command output
	re.Contains(outputStr, fmt.Sprintf("Successfully set placement rules for keyspace %d table %d", keyspaceID, tableID))
	re.Contains(outputStr, fmt.Sprintf("%s=%s", labelKey, labelValue))

	// Verify the bundle was created
	groupID := fmt.Sprintf("keyspace-%d-table-%d", keyspaceID, tableID)
	bundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Equal(groupID, bundle.ID)
	re.Len(bundle.Rules, 2)
	// Verify the label constraints
	re.Equal(labelKey, bundle.Rules[0].LabelConstraints[0].Key)
	re.Equal(labelValue, bundle.Rules[0].LabelConstraints[0].Values[0])

	// Verify rules are applied to the table region
	handler := leaderServer.GetServer().GetHandler()
	regionFit, err := handler.CheckRegionPlacementRule(tableRegion)
	re.NoError(err)
	re.NotNil(regionFit)
	re.Len(regionFit.RuleFits, 1, "should only have one rule fit due to Override=true")
	re.Equal(groupID, regionFit.RuleFits[0].Rule.GroupID, "applied rule should be from our custom table group")

	// Revert the table-level placement rules
	args = []string{"-u", suite.pdAddr, "keyspace", "revert-placement", strconv.Itoa(int(keyspaceID)), "--table-id", strconv.FormatInt(tableID, 10)}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	outputStr = string(output)

	// The command should succeed
	re.Contains(outputStr, fmt.Sprintf("Successfully reverted placement rules for keyspace %d table %d", keyspaceID, tableID))

	// Verify the bundle was deleted - check that the group no longer exists
	group := leaderServer.GetRaftCluster().GetRuleManager().GetRuleGroup(groupID)
	re.Nil(group, "rule group should be deleted")
	// Also verify that there are no rules for this group
	bundle = leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(groupID)
	re.Empty(bundle.Rules, "bundle rules should be empty")

	// Verify rules are no longer applied to the table region after revert
	regionFitAfter, err := handler.CheckRegionPlacementRule(tableRegion)
	re.NoError(err)
	// After revert, should use default rules (not the table-specific rules)
	re.Len(regionFitAfter.RuleFits, 1, "should have only one default rule applied after revert")
	re.NotEqual(groupID, regionFitAfter.RuleFits[0].Rule.GroupID, "should use default rule, not table-specific rule")
}

func (suite *keyspaceTestSuite) TestSetAndRevertPlacementBothLevels() {
	re := suite.Require()
	leaderServer := suite.cluster.GetLeaderServer()

	// Add stores with labels
	for i := 1; i <= 3; i++ {
		store := &metapb.Store{
			Id:      uint64(i),
			Address: fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Labels: []*metapb.StoreLabel{
				{Key: "zone", Value: "east"},
				{Key: "disk", Value: "ssd"},
			},
			State:         metapb.StoreState_Up,
			LastHeartbeat: 0,
		}
		pdTests.MustPutStore(re, suite.cluster, store)
	}

	// Create a test keyspace
	keyspaceName := "test_keyspace_both_levels"
	meta := suite.mustCreateKeyspace(api.CreateKeyspaceParams{Name: keyspaceName})
	keyspaceID := meta.GetId()

	// Create regions for both keyspace and table levels
	keyspaceRegion := suite.createKeyspaceRegion(keyspaceID)
	re.NotNil(keyspaceRegion, "keyspace region should exist")

	tableID := int64(300)
	tableRegion := suite.createTableRegion(keyspaceID, tableID)
	re.NotNil(tableRegion, "table region should exist")

	// Set keyspace-level placement rules
	args := []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "zone=east"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), fmt.Sprintf("Successfully set placement rules for keyspace %d", keyspaceID))

	// Set table-level placement rules for a specific table
	args = []string{"-u", suite.pdAddr, "keyspace", "set-placement", strconv.Itoa(int(keyspaceID)), "--table-id", strconv.FormatInt(tableID, 10), "disk=ssd"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), fmt.Sprintf("Successfully set placement rules for keyspace %d table %d", keyspaceID, tableID))

	// Verify both bundles exist
	keyspaceGroupID := fmt.Sprintf("keyspace-%d", keyspaceID)
	tableGroupID := fmt.Sprintf("keyspace-%d-table-%d", keyspaceID, tableID)

	keyspaceBundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(keyspaceGroupID)
	re.NotEmpty(keyspaceBundle.Rules, "keyspace-level bundle should exist")
	re.Equal(keyspaceGroupID, keyspaceBundle.ID)

	tableBundle := leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(tableGroupID)
	re.NotEmpty(tableBundle.Rules, "table-level bundle should exist")
	re.Equal(tableGroupID, tableBundle.ID)

	// Verify keyspace-level rules have zone=east constraint
	re.Equal("zone", keyspaceBundle.Rules[0].LabelConstraints[0].Key)
	re.Equal("east", keyspaceBundle.Rules[0].LabelConstraints[0].Values[0])

	// Verify table-level rules have disk=ssd constraint
	re.Equal("disk", tableBundle.Rules[0].LabelConstraints[0].Key)
	re.Equal("ssd", tableBundle.Rules[0].LabelConstraints[0].Values[0])

	// Verify rules are applied to respective regions
	handler := leaderServer.GetServer().GetHandler()

	// Keyspace region should use keyspace-level rules
	keyspaceRegionFit, err := handler.CheckRegionPlacementRule(keyspaceRegion)
	re.NoError(err)
	re.NotNil(keyspaceRegionFit)
	re.Len(keyspaceRegionFit.RuleFits, 1, "keyspace region should have one rule fit")
	re.Equal(keyspaceGroupID, keyspaceRegionFit.RuleFits[0].Rule.GroupID, "keyspace region should use keyspace-level rule")

	// Table region should use table-level rules
	tableRegionFit, err := handler.CheckRegionPlacementRule(tableRegion)
	re.NoError(err)
	re.NotNil(tableRegionFit)
	re.Len(tableRegionFit.RuleFits, 1, "table region should have one rule fit")
	re.Equal(tableGroupID, tableRegionFit.RuleFits[0].Rule.GroupID, "table region should use table-level rule")

	// Revert only the table-level placement rules
	args = []string{"-u", suite.pdAddr, "keyspace", "revert-placement", strconv.Itoa(int(keyspaceID)), "--table-id", strconv.FormatInt(tableID, 10)}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), fmt.Sprintf("Successfully reverted placement rules for keyspace %d table %d", keyspaceID, tableID))

	// Verify table-level bundle is deleted but keyspace-level bundle still exists
	tableBundle = leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(tableGroupID)
	re.Empty(tableBundle.Rules, "table-level bundle should be deleted")

	keyspaceBundle = leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(keyspaceGroupID)
	re.NotEmpty(keyspaceBundle.Rules, "keyspace-level bundle should still exist")

	// Verify table region now uses default rules (not table-specific)
	tableRegionFitAfter, err := handler.CheckRegionPlacementRule(tableRegion)
	re.NoError(err)
	re.NotEqual(tableGroupID, tableRegionFitAfter.RuleFits[0].Rule.GroupID, "table region should use default rule after revert")

	// Verify keyspace region still uses keyspace-level rules
	keyspaceRegionFitAfter, err := handler.CheckRegionPlacementRule(keyspaceRegion)
	re.NoError(err)
	re.Equal(keyspaceGroupID, keyspaceRegionFitAfter.RuleFits[0].Rule.GroupID, "keyspace region should still use keyspace-level rule")

	// Revert keyspace-level placement rules
	args = []string{"-u", suite.pdAddr, "keyspace", "revert-placement", strconv.Itoa(int(keyspaceID))}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), fmt.Sprintf("Successfully reverted placement rules for keyspace %d", keyspaceID))

	// Verify keyspace-level bundle is also deleted
	keyspaceBundle = leaderServer.GetRaftCluster().GetRuleManager().GetGroupBundle(keyspaceGroupID)
	re.Empty(keyspaceBundle.Rules, "keyspace-level bundle should be deleted")

	// Verify both regions now use default rules
	keyspaceRegionFitFinal, err := handler.CheckRegionPlacementRule(keyspaceRegion)
	re.NoError(err)
	re.NotEqual(keyspaceGroupID, keyspaceRegionFitFinal.RuleFits[0].Rule.GroupID, "keyspace region should use default rule after revert")
}
