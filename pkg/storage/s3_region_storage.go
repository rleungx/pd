// Copyright 2025 TiKV Project Authors.
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

package storage

import (
	"context"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// S3RegionStorage is a storage for the PD region meta information based on S3,
// which will override the default implementation of the `endpoint.RegionStorage`.
type S3RegionStorage struct {
	kv.Base
	backend *s3Backend
}

var _ endpoint.RegionStorage = (*S3RegionStorage)(nil)

// NewRegionStorageWithS3Backend creates a specialized storage to
// store region meta information based on an S3 backend.
func NewRegionStorageWithS3Backend(
	ctx context.Context,
	s3Config *kv.S3Config,
	ekm *encryption.Manager,
) (*S3RegionStorage, error) {
	s3Backend, err := newS3Backend(ctx, s3Config, ekm)
	if err != nil {
		return nil, err
	}
	return newS3RegionStorage(s3Backend), nil
}

func newS3RegionStorage(backend *s3Backend) *S3RegionStorage {
	return &S3RegionStorage{Base: backend.Base, backend: backend}
}

// LoadRegion implements the `endpoint.RegionStorage` interface.
func (s *S3RegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return s.backend.LoadRegion(regionID, region)
}

// LoadRegions implements the `endpoint.RegionStorage` interface.
func (s *S3RegionStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	return s.backend.LoadRegions(ctx, f)
}

// SaveRegion implements the `endpoint.RegionStorage` interface.
// It will encrypt the region if encryption manager is available, then save it in batch.
func (s *S3RegionStorage) SaveRegion(region *metapb.Region) error {
	var regionToSave = region

	// Encrypt region if encryption manager is available
	if s.backend.ekm != nil {
		encryptedRegion, err := encryption.EncryptRegion(region, s.backend.ekm)
		if err != nil {
			return err
		}
		regionToSave = encryptedRegion
	}

	value, err := proto.Marshal(regionToSave)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.backend.SaveIntoBatch(keypath.RegionPath(region.GetId()), value)
}

// DeleteRegion implements the `endpoint.RegionStorage` interface.
func (s *S3RegionStorage) DeleteRegion(region *metapb.Region) error {
	return s.backend.Remove(keypath.RegionPath(region.GetId()))
}

// Flush implements the `endpoint.RegionStorage` interface.
func (s *S3RegionStorage) Flush() error {
	return s.backend.Flush()
}

// Close implements the `endpoint.RegionStorage` interface.
func (s *S3RegionStorage) Close() error {
	return s.backend.Close()
}
