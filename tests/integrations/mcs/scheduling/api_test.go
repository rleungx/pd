package scheduling_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	_ "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

var testDialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type apiTestSuite struct {
	suite.Suite
	ctx              context.Context
	cleanupFunc      testutil.CleanupFunc
	cluster          *tests.TestCluster
	server           *tests.TestServer
	backendEndpoints string
	dialClient       *http.Client
}

func TestAPI(t *testing.T) {
	suite.Run(t, &apiTestSuite{})
}

func (suite *apiTestSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestAPICluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	suite.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.dialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	suite.cleanupFunc = func() {
		cancel()
	}
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.backendEndpoints)
	suite.NoError(err)
	suite.cluster.SetSchedulingCluster(tc)
	tc.WaitForPrimaryServing(suite.Require())
	tc.GetPrimaryServer().GetCluster().SetPrepared()
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", "return(true)"))
}

func (suite *apiTestSuite) TearDownTest() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
	suite.cluster.Destroy()
	suite.cleanupFunc()
}

func (suite *apiTestSuite) TestGetCheckerByName() {
	re := suite.Require()
	testCases := []struct {
		name string
	}{
		{name: "learner"},
		{name: "replica"},
		{name: "rule"},
		{name: "split"},
		{name: "merge"},
		{name: "joint-state"},
	}

	s := suite.cluster.GetSchedulingPrimaryServer()
	urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/checkers", s.GetAddr())
	co := s.GetCoordinator()

	for _, testCase := range testCases {
		name := testCase.name
		// normal run
		resp := make(map[string]interface{})
		err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.False(resp["paused"].(bool))
		// paused
		err = co.PauseOrResumeChecker(name, 30)
		suite.NoError(err)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.True(resp["paused"].(bool))
		// resumed
		err = co.PauseOrResumeChecker(name, 1)
		suite.NoError(err)
		time.Sleep(time.Second)
		resp = make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, name), &resp)
		suite.NoError(err)
		suite.False(resp["paused"].(bool))
	}
}

func (suite *apiTestSuite) TestAPIForward() {
	re := suite.Require()

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", suite.backendEndpoints)
	var respSlice []string
	var resp map[string]interface{}
	testutil.Eventually(re, func() bool {
		return suite.cluster.GetLeaderServer().GetServer().GetRaftCluster().IsServiceIndependent(utils.SchedulingServiceName)
	})
	// Test operators
	err := testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &respSlice,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	re.Len(respSlice, 0)

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), []byte(``),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/2"),
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators/records"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test checker
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	suite.False(resp["paused"].(bool))

	input := make(map[string]interface{})
	input["delay"] = 10
	pauseArgs, err := json.Marshal(input)
	suite.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "checker/merge"), pauseArgs,
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)

	// Test scheduler:
	// Need to redirect:
	//	"/schedulers", http.MethodGet
	//	"/schedulers/{name}", http.MethodPost
	//	"/schedulers/diagnostic/{name}", http.MethodGet
	// 	"/scheduler-config/", http.MethodGet
	// 	"/scheduler-config/{name}/list", http.MethodGet
	// 	"/scheduler-config/{name}/roles", http.MethodGet
	// Should not redirect:
	//	"/schedulers", http.MethodPost
	//	"/schedulers/{name}", http.MethodDelete
	testutil.Eventually(re, func() bool {
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), &respSlice,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		re.NoError(err)
		return slice.Contains(respSlice, "balance-leader-scheduler")
	})

	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	suite.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"), pauseArgs,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/diagnostic/balance-leader-scheduler"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "scheduler-config"), &resp,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)
	re.Contains(resp, "balance-leader-scheduler")
	re.Contains(resp, "balance-witness-scheduler")
	re.Contains(resp, "balance-hot-region-scheduler")

	schedulers := []string{
		"balance-leader-scheduler",
		"balance-witness-scheduler",
		"balance-hot-region-scheduler",
	}
	for _, schedulerName := range schedulers {
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s/%s/%s", urlPrefix, "scheduler-config", schedulerName, "list"), &resp,
			testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
		suite.NoError(err)
	}

	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers"), pauseArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "schedulers/balance-leader-scheduler"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	// Test hotspot
	var hotRegions statistics.StoreHotPeersInfos
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/write"), &hotRegions,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/read"), &hotRegions,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var stores handler.HotStoreStats
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/stores"), &stores,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var history storage.HistoryHotRegions
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "hotspot/regions/history"), &history,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test region label
	var labelRules []*labeler.LabelRule
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules"), &labelRules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSONWithBody(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rules/ids"), []byte(`["rule1", "rule3"]`),
		&labelRules, testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/region-label/rule/rule1"), nil,
		testutil.StatusNotOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1"), nil,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/label/key"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "region/id/1/labels"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test Region
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`[{"start_key":"%s", "end_key": "%s"}, {"start_key":"%s", "end_key": "%s"}]`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")), hex.EncodeToString([]byte("a4")), hex.EncodeToString([]byte("a6")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/accelerate-schedule/batch"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	body = fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/scatter"), []byte(body),
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)
	body = fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "regions/split"), []byte(body),
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a2"))), nil,
		testutil.StatusOK(re), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	suite.NoError(err)
	// Test rules: only forward `GET` request
	var rules []*placement.Rule
	tests.MustPutRegion(re, suite.cluster, 2, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	rules = []*placement.Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           "voter",
			Count:          3,
			LocationLabels: []string{},
		},
	}
	rulesArgs, err := json.Marshal(rules)
	suite.NoError(err)

	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/batch"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/group/pd"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	var fit placement.RegionFit
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/region/2/detail"), &fit,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules/key/0000000000000001"), &rules,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule/pd/2"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group/pd"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_group"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rule_groups"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	err = testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"),
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	err = testutil.CheckPostJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), rulesArgs,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)

	// test redirect is disabled
	err = testutil.CheckGetJSON(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), nil,
		testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", urlPrefix, "config/placement-rule/pd"), http.NoBody)
	re.NoError(err)
	req.Header.Set(apiutil.XForbiddenForwardToMicroServiceHeader, "true")
	httpResp, err := testDialClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, httpResp.StatusCode)
	defer httpResp.Body.Close()
	re.Empty(httpResp.Header.Get(apiutil.XForwardedToMicroServiceHeader))
}

func (suite *apiTestSuite) TestConfig() {
	checkConfig := func(cluster *tests.TestCluster) {
		re := suite.Require()
		s := cluster.GetSchedulingPrimaryServer()
		testutil.Eventually(re, func() bool {
			return s.IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
		addr := s.GetAddr()
		urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/config", addr)

		var cfg config.Config
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		suite.Equal(cfg.GetListenAddr(), s.GetConfig().GetListenAddr())
		suite.Equal(cfg.Schedule.LeaderScheduleLimit, s.GetConfig().Schedule.LeaderScheduleLimit)
		suite.Equal(cfg.Schedule.EnableCrossTableMerge, s.GetConfig().Schedule.EnableCrossTableMerge)
		suite.Equal(cfg.Replication.MaxReplicas, s.GetConfig().Replication.MaxReplicas)
		suite.Equal(cfg.Replication.LocationLabels, s.GetConfig().Replication.LocationLabels)
		suite.Equal(cfg.DataDir, s.GetConfig().DataDir)
		testutil.Eventually(re, func() bool {
			// wait for all schedulers to be loaded in scheduling server.
			return len(cfg.Schedule.SchedulersPayload) == 5
		})
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-leader-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-region-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-hot-region-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "balance-witness-scheduler")
		suite.Contains(cfg.Schedule.SchedulersPayload, "transfer-witness-leader-scheduler")
	}
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInAPIMode(checkConfig)
}

func TestConfigForward(t *testing.T) {
	re := require.New(t)
	checkConfigForward := func(cluster *tests.TestCluster) {
		sche := cluster.GetSchedulingPrimaryServer()
		opts := sche.GetPersistConfig()
		var cfg map[string]interface{}
		addr := cluster.GetLeaderServer().GetAddr()
		urlPrefix := fmt.Sprintf("%s/pd/api/v1/config", addr)

		// Test config forward
		// Expect to get same config in scheduling server and api server
		testutil.Eventually(re, func() bool {
			testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
			re.Equal(cfg["schedule"].(map[string]interface{})["leader-schedule-limit"],
				float64(opts.GetLeaderScheduleLimit()))
			re.Equal(cfg["replication"].(map[string]interface{})["max-replicas"],
				float64(opts.GetReplicationConfig().MaxReplicas))
			schedulers := cfg["schedule"].(map[string]interface{})["schedulers-payload"].(map[string]interface{})
			return len(schedulers) == 5
		})

		// Test to change config in api server
		// Expect to get new config in scheduling server and api server
		reqData, err := json.Marshal(map[string]interface{}{
			"max-replicas": 4,
		})
		re.NoError(err)
		err = testutil.CheckPostJSON(testDialClient, urlPrefix, reqData, testutil.StatusOK(re))
		re.NoError(err)
		testutil.Eventually(re, func() bool {
			testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
			return cfg["replication"].(map[string]interface{})["max-replicas"] == 4. &&
				opts.GetReplicationConfig().MaxReplicas == 4.
		})

		// Test to change config only in scheduling server
		// Expect to get new config in scheduling server but not old config in api server
		opts.GetScheduleConfig().LeaderScheduleLimit = 100
		re.Equal(100, int(opts.GetLeaderScheduleLimit()))
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		re.Equal(100., cfg["schedule"].(map[string]interface{})["leader-schedule-limit"])
		opts.GetReplicationConfig().MaxReplicas = 5
		re.Equal(5, int(opts.GetReplicationConfig().MaxReplicas))
		testutil.ReadGetJSON(re, testDialClient, urlPrefix, &cfg)
		re.Equal(5., cfg["replication"].(map[string]interface{})["max-replicas"])
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkConfigForward)
}

func TestAdminRegionCache(t *testing.T) {
	re := require.New(t)
	checkAdminRegionCache := func(cluster *tests.TestCluster) {
		r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r1)
		r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r2)
		r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r3)

		schedulingServer := cluster.GetSchedulingPrimaryServer()
		re.Equal(3, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))

		addr := schedulingServer.GetAddr()
		urlPrefix := fmt.Sprintf("%s/scheduling/api/v1/admin/cache/regions", addr)
		err := testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
		re.NoError(err)
		re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))

		err = testutil.CheckDelete(testDialClient, urlPrefix, testutil.StatusOK(re))
		re.NoError(err)
		re.Equal(0, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkAdminRegionCache)
}

func TestAdminRegionCacheForward(t *testing.T) {
	re := require.New(t)
	checkAdminRegionCache := func(cluster *tests.TestCluster) {
		r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r1)
		r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r2)
		r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetRegionConfVer(100), core.SetRegionVersion(100))
		tests.MustPutRegionInfo(re, cluster, r3)

		apiServer := cluster.GetLeaderServer().GetServer()
		schedulingServer := cluster.GetSchedulingPrimaryServer()
		re.Equal(3, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
		re.Equal(3, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)

		addr := cluster.GetLeaderServer().GetAddr()
		urlPrefix := fmt.Sprintf("%s/pd/api/v1/admin/cache/region", addr)
		err := testutil.CheckDelete(testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "30"), testutil.StatusOK(re))
		re.NoError(err)
		re.Equal(2, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
		re.Equal(2, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)

		err = testutil.CheckDelete(testDialClient, urlPrefix+"s", testutil.StatusOK(re))
		re.NoError(err)
		re.Equal(0, schedulingServer.GetCluster().GetRegionCount([]byte{}, []byte{}))
		re.Equal(0, apiServer.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count)
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkAdminRegionCache)
}

func TestMetrics(t *testing.T) {
	re := require.New(t)
	checkMetrics := func(cluster *tests.TestCluster) {
		s := cluster.GetSchedulingPrimaryServer()
		testutil.Eventually(re, func() bool {
			return s.IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
		resp, err := http.Get(s.GetConfig().GetAdvertiseListenAddr() + "/metrics")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Contains(string(respBytes), "scheduling_server_info")
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkMetrics)
}

func TestStatus(t *testing.T) {
	re := require.New(t)
	checkStatus := func(cluster *tests.TestCluster) {
		s := cluster.GetSchedulingPrimaryServer()
		testutil.Eventually(re, func() bool {
			return s.IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
		resp, err := http.Get(s.GetConfig().GetAdvertiseListenAddr() + "/status")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		var status versioninfo.Status
		re.NoError(json.Unmarshal(respBytes, &status))
		re.Equal(versioninfo.PDBuildTS, status.BuildTS)
		re.Equal(versioninfo.PDGitHash, status.GitHash)
		re.Equal(versioninfo.PDReleaseVersion, status.Version)
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInAPIMode(checkStatus)
}

func TestFollowerForward(t *testing.T) {
	re := require.New(t)
	checkFollowerForward := func(cluster *tests.TestCluster) {
		leaderAddr := cluster.GetLeaderServer().GetAddr()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		follower, err := cluster.JoinAPIServer(ctx)
		re.NoError(err)
		re.NoError(follower.Run())
		re.NotEmpty(cluster.WaitLeader())

		followerAddr := follower.GetAddr()
		if cluster.GetLeaderServer().GetAddr() != leaderAddr {
			followerAddr = leaderAddr
		}

		urlPrefix := fmt.Sprintf("%s/pd/api/v1", followerAddr)
		rules := []*placement.Rule{}
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			// follower will forward to scheduling server directly
			re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
			err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
				testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"),
			)
			re.NoError(err)
		} else {
			// follower will forward to leader server
			re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
			err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config/rules"), &rules,
				testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader),
			)
			re.NoError(err)
		}

		// follower will forward to leader server
		re.NotEqual(cluster.GetLeaderServer().GetAddr(), followerAddr)
		results := make(map[string]interface{})
		err = testutil.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s/%s", urlPrefix, "config"), &results,
			testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader),
		)
		re.NoError(err)
	}
	env := tests.NewSchedulingTestEnvironment(t)
	env.RunTestInTwoModes(checkFollowerForward)
}

func (suite *apiTestSuite) TestStores() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInAPIMode(suite.checkStores)
}

func (suite *apiTestSuite) checkStores(cluster *tests.TestCluster) {
	re := suite.Require()
	stores := []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        4,
			Address:   "tikv4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:        6,
			Address:   "tikv6",
			State:     metapb.StoreState_Offline,
			NodeState: metapb.NodeState_Removing,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:        7,
			Address:   "tikv7",
			State:     metapb.StoreState_Tombstone,
			NodeState: metapb.NodeState_Removed,
			Version:   "2.0.0",
		},
	}
	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	// Test /stores
	apiServerAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/stores", apiServerAddr)
	var resp map[string]interface{}
	err := testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["stores"].([]interface{}), 3)
	scheServerAddr := cluster.GetSchedulingPrimaryServer().GetAddr()
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["stores"].([]interface{}), 3)
	// Test /stores/{id}
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/1", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv1", resp["store"].(map[string]interface{})["address"])
	re.Equal("Up", resp["store"].(map[string]interface{})["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/6", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv6", resp["store"].(map[string]interface{})["address"])
	re.Equal("Offline", resp["store"].(map[string]interface{})["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/7", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal("tikv7", resp["store"].(map[string]interface{})["address"])
	re.Equal("Tombstone", resp["store"].(map[string]interface{})["state_name"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/stores/233", scheServerAddr)
	testutil.CheckGetJSON(testDialClient, urlPrefix, nil,
		testutil.Status(re, http.StatusNotFound), testutil.StringContain(re, "not found"))
}

func (suite *apiTestSuite) TestRegions() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInAPIMode(suite.checkRegions)
}

func (suite *apiTestSuite) checkRegions(cluster *tests.TestCluster) {
	re := suite.Require()
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"))
	tests.MustPutRegion(re, cluster, 2, 2, []byte("c"), []byte("d"))
	tests.MustPutRegion(re, cluster, 3, 1, []byte("e"), []byte("f"))
	// Test /regions
	apiServerAddr := cluster.GetLeaderServer().GetAddr()
	urlPrefix := fmt.Sprintf("%s/pd/api/v1/regions", apiServerAddr)
	var resp map[string]interface{}
	err := testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["regions"].([]interface{}), 3)
	scheServerAddr := cluster.GetSchedulingPrimaryServer().GetAddr()
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3, int(resp["count"].(float64)))
	re.Len(resp["regions"].([]interface{}), 3)
	// Test /regions/{id} and /regions/count
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/1", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	key := fmt.Sprintf("%x", "a")
	re.Equal(key, resp["start_key"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/count", scheServerAddr)
	err = testutil.ReadGetJSON(re, testDialClient, urlPrefix, &resp)
	re.NoError(err)
	re.Equal(3., resp["count"])
	urlPrefix = fmt.Sprintf("%s/scheduling/api/v1/regions/233", scheServerAddr)
	testutil.CheckGetJSON(testDialClient, urlPrefix, nil,
		testutil.Status(re, http.StatusNotFound), testutil.StringContain(re, "not found"))
}
