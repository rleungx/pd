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

package filter

import "github.com/tikv/pd/server/core"

// regionStatus is the current status of a region.
type regionStatus int

// These are the possible values for a storeStatus.
const (
	_ regionStatus = iota
	regionIsHot
	regionNoLeader
)

// distinctScoreFilter ensures that distinct score will not decrease.
type hotRegionFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	policy    string
	safeScore float64
	srcStore  uint64
}
