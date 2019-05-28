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

package operator

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api/helper"
	"github.com/pingcap/pd/server/api/util"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct {
	svr       *server.Server
	cleanup   func()
	urlPrefix string
}

func (s *testOperatorSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = helper.MustNewServer(c)
	helper.MustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s/pd/api/v1", addr)

	helper.MustBootstrapCluster(c, s.svr)
}

func (s *testOperatorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testOperatorSuite) TestAddRemovePeer(c *C) {
	helper.MustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
	helper.MustPutStore(c, s.svr, 2, metapb.StoreState_Up, nil)

	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	regionInfo := core.NewRegionInfo(region, peer1)
	helper.MustRegionHeartbeat(c, s.svr, regionInfo)

	regionURL := fmt.Sprintf("%s/operators/%d", s.urlPrefix, region.GetId())
	operator := mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "operator not found"), IsTrue)

	helper.MustPutStore(c, s.svr, 3, metapb.StoreState_Up, nil)
	err := util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 1 on store 3"), IsTrue)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)

	err = util.DoDelete(regionURL)
	c.Assert(err, IsNil)

	err = util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)
	c.Assert(strings.Contains(operator, "remove peer on store 2"), IsTrue)

	err = util.DoDelete(regionURL)
	c.Assert(err, IsNil)

	helper.MustPutStore(c, s.svr, 4, metapb.StoreState_Up, nil)
	err = util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 2 on store 4"), IsTrue)

	// Fail to add peer to tombstone store.
	err = s.svr.GetRaftCluster().BuryStore(3, true)
	c.Assert(err, IsNil)
	err = util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, NotNil)
	err = util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`))
	c.Assert(err, NotNil)
	err = util.PostJSON(fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`))
	c.Assert(err, NotNil)

}

func mustReadURL(c *C, url string) string {
	res, err := http.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	return string(data)
}
