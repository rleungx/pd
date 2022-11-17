// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"go.uber.org/zap"
)

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	tree *btree.BTreeG[*RegionInfo]
	// Statistics
	totalSize           int64
	totalWriteBytesRate float64
	totalWriteKeysRate  float64
}

func newRegionTree() *regionTree {
	return &regionTree{
		tree:                btree.NewG[*RegionInfo](defaultBTreeDegree),
		totalSize:           0,
		totalWriteBytesRate: 0,
		totalWriteKeysRate:  0,
	}
}

func (t *regionTree) length() int {
	if t == nil {
		return 0
	}
	return t.tree.Len()
}

// getOverlaps gets the regions which are overlapped with the specified region range.
func (t *regionTree) getOverlaps(region *RegionInfo) []*RegionInfo {
	result := t.overlaps(region)
	overlaps := make([]*RegionInfo, len(result))
	copy(overlaps, result)

	return overlaps
}

// GetOverlaps returns the range items that has some intersections with the given items.
func (t *regionTree) overlaps(region *RegionInfo) []*RegionInfo {
	// note that Find() gets the last item that is less or equal than the item.
	// in the case: |_______a_______|_____b_____|___c___|
	// new item is     |______d______|
	// Find() will return RangeItem of item_a
	// and both startKey of item_a and item_b are less than endKey of item_d,
	// thus they are regarded as overlapped items.
	result := t.find(region)
	if result == nil {
		result = region
	}
	endKey := region.GetEndKey()
	var overlaps []*RegionInfo
	t.tree.AscendGreaterOrEqual(result, func(i *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(endKey, i.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, i)
		return true
	})
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(region *RegionInfo) []*RegionInfo {
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	overlaps := t.overlaps(region)
	for _, old := range overlaps {
		t.tree.Delete(old)
	}
	t.tree.ReplaceOrInsert(region)
	result := make([]*RegionInfo, len(overlaps))
	for i, overlap := range overlaps {
		old := overlap
		result[i] = old
		log.Debug("overlapping region",
			zap.Uint64("region-id", old.GetID()),
			logutil.ZapRedactStringer("delete-region", RegionToHexMeta(old.GetMeta())),
			logutil.ZapRedactStringer("update-region", RegionToHexMeta(region.GetMeta())))
		t.totalSize -= old.approximateSize
		regionWriteBytesRate, regionWriteKeysRate = old.GetWriteRate()
		t.totalWriteBytesRate -= regionWriteBytesRate
		t.totalWriteKeysRate -= regionWriteKeysRate
	}

	return result
}

// updateStat is used to update statistics when RegionInfo is directly replaced.
func (t *regionTree) updateStat(origin *RegionInfo, region *RegionInfo) {
	t.totalSize += region.approximateSize
	regionWriteBytesRate, regionWriteKeysRate := region.GetWriteRate()
	t.totalWriteBytesRate += regionWriteBytesRate
	t.totalWriteKeysRate += regionWriteKeysRate

	t.totalSize -= origin.approximateSize
	regionWriteBytesRate, regionWriteKeysRate = origin.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionTree) remove(region *RegionInfo) {
	if t.length() == 0 {
		return
	}
	result := t.find(region)
	if result == nil || result.GetID() != region.GetID() {
		return
	}

	t.totalSize -= result.GetApproximateSize()
	regionWriteBytesRate, regionWriteKeysRate := result.GetWriteRate()
	t.totalWriteBytesRate -= regionWriteBytesRate
	t.totalWriteKeysRate -= regionWriteKeysRate
	t.tree.Delete(region)
}

// search returns a region that contains the key.
func (t *regionTree) search(regionKey []byte) *RegionInfo {
	region := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	result := t.find(region)
	if result == nil {
		return nil
	}
	return result
}

// searchPrev returns the previous region of the region where the regionKey is located.
func (t *regionTree) searchPrev(regionKey []byte) *RegionInfo {
	curRegion := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	curRegionItem := t.find(curRegion)
	if curRegionItem == nil {
		return nil
	}
	prevRegionItem, _ := t.getAdjacentRegions(curRegionItem)
	if prevRegionItem == nil {
		return nil
	}
	if !bytes.Equal(prevRegionItem.GetEndKey(), curRegionItem.GetStartKey()) {
		return nil
	}
	return prevRegionItem
}

// find returns the range item contains the start key.
func (t *regionTree) find(item *RegionInfo) *RegionInfo {
	var result *RegionInfo
	t.tree.DescendLessOrEqual(item, func(i *RegionInfo) bool {
		result = i
		return false
	})

	if result == nil || !result.Contains(item.GetStartKey()) {
		return nil
	}

	return result
}

// scanRage scans from the first region containing or behind the start key
// until f return false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// find if there is a region with key range [s, d), s < startKey < d
	fn := func(item *RegionInfo) bool {
		r := item
		return f(r)
	}
	start := region
	startItem := t.find(start)
	if startItem == nil {
		startItem = start
	}
	t.tree.AscendGreaterOrEqual(startItem, func(item *RegionInfo) bool {
		return fn(item)
	})
}

func (t *regionTree) scanRanges() []*RegionInfo {
	if t.length() == 0 {
		return nil
	}
	var res []*RegionInfo
	t.scanRange([]byte(""), func(region *RegionInfo) bool {
		res = append(res, region)
		return true
	})
	return res
}

func (t *regionTree) getAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	item := &RegionInfo{meta: &metapb.Region{StartKey: region.GetStartKey()}}
	prevItem, nextItem := t.getAdjacentItem(item)
	var prev, next *RegionInfo
	if prevItem != nil {
		prev = prevItem
	}
	if nextItem != nil {
		next = nextItem
	}
	return prev, next
}

// GetAdjacentItem returns the adjacent range item.
func (t *regionTree) getAdjacentItem(item *RegionInfo) (prev *RegionInfo, next *RegionInfo) {
	t.tree.AscendGreaterOrEqual(item, func(i *RegionInfo) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		next = i
		return false
	})
	t.tree.DescendLessOrEqual(item, func(i *RegionInfo) bool {
		if bytes.Equal(item.GetStartKey(), i.GetStartKey()) {
			return true
		}
		prev = i
		return false
	})
	return prev, next
}

// RandomRegion is used to get a random region within ranges.
func (t *regionTree) RandomRegion(ranges []KeyRange) *RegionInfo {
	if t.length() == 0 {
		return nil
	}

	if len(ranges) == 0 {
		ranges = []KeyRange{NewKeyRange("", "")}
	}

	for _, i := range rand.Perm(len(ranges)) {
		var endIndex int
		startKey, endKey := ranges[i].StartKey, ranges[i].EndKey
		startRegion, startIndex := t.tree.GetWithIndex(&RegionInfo{meta: &metapb.Region{StartKey: startKey}})

		if len(endKey) != 0 {
			_, endIndex = t.tree.GetWithIndex(&RegionInfo{meta: &metapb.Region{StartKey: endKey}})
		} else {
			endIndex = t.tree.Len()
		}

		// Consider that the item in the tree may not be continuous,
		// we need to check if the previous item contains the key.
		if startIndex != 0 && startRegion == nil && t.tree.GetAt(startIndex-1).Contains(startKey) {
			startIndex--
		}

		if endIndex <= startIndex {
			if len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
				log.Error("wrong range keys",
					logutil.ZapRedactString("start-key", string(HexRegionKey(startKey))),
					logutil.ZapRedactString("end-key", string(HexRegionKey(endKey))),
					errs.ZapError(errs.ErrWrongRangeKeys))
			}
			continue
		}
		index := rand.Intn(endIndex-startIndex) + startIndex
		region := t.tree.GetAt(index)
		if region.isInvolved(startKey, endKey) {
			return region
		}
	}

	return nil
}

func (t *regionTree) RandomRegions(n int, ranges []KeyRange) []*RegionInfo {
	if t.length() == 0 {
		return nil
	}

	regions := make([]*RegionInfo, 0, n)

	for i := 0; i < n; i++ {
		if region := t.RandomRegion(ranges); region != nil {
			regions = append(regions, region)
		}
	}
	return regions
}

func (t *regionTree) TotalSize() int64 {
	if t.length() == 0 {
		return 0
	}
	return t.totalSize
}

func (t *regionTree) TotalWriteRate() (bytesRate, keysRate float64) {
	if t.length() == 0 {
		return 0, 0
	}
	return t.totalWriteBytesRate, t.totalWriteKeysRate
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
