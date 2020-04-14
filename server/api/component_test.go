// Copyright 2020 PingCAP, Inc.
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

package api

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
)

var _ = Suite(&testComponentSuite{})

type testComponentSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testComponentSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testComponentSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testComponentSuite) TestRegister(c *C) {
	addr := fmt.Sprintf("%s/component", s.urlPrefix)
	output := make(map[string][]string)
	err := readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(len(output), Equals, 0)

	addr1 := fmt.Sprintf("%s/component/c1", s.urlPrefix)
	var output1 []string
	err = readJSON(addr1, &output)
	c.Assert(strings.Contains(err.Error(), "404"), IsTrue)
	c.Assert(len(output1), Equals, 0)

	addr2 := fmt.Sprintf("%s/component/register", s.urlPrefix)
	reqs := []map[string]string{
		{"component": "c1", "addr": "127.0.0.1:1"},
		{"component": "c1", "addr": "127.0.0.1:2"},
		{"component": "c2", "addr": "127.0.0.1:3"},
	}
	for _, req := range reqs {
		postData, err := json.Marshal(req)
		c.Assert(err, IsNil)
		err = postJSON(addr2, postData)
		c.Assert(err, IsNil)
	}

	expected := map[string][]string{
		"c1": {"127.0.0.1:1", "127.0.0.1:2"},
		"c2": {"127.0.0.1:3"},
	}

	output = make(map[string][]string)
	err = readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(output, DeepEquals, expected)

	expected1 := []string{"127.0.0.1:1", "127.0.0.1:2"}
	var output2 []string
	err = readJSON(addr1, &output2)
	c.Assert(err, IsNil)
	c.Assert(output2, DeepEquals, expected1)

	addr3 := fmt.Sprintf("%s/component/c2", s.urlPrefix)
	expected2 := []string{"127.0.0.1:3"}
	var output3 []string
	err = readJSON(addr3, &output3)
	c.Assert(err, IsNil)
	c.Assert(output3, DeepEquals, expected2)
}
