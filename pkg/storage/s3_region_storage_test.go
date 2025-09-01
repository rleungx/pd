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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

// The following test requires S3 credentials and configuration to be set via environment variables:
// - AWS_ACCESS_KEY_ID: AWS access key
// - AWS_SECRET_ACCESS_KEY: AWS secret key
// - AWS_REGION: AWS region (optional, defaults to us-east-1)
// - S3_ENDPOINT: S3 endpoint URL (optional, for S3-compatible services like MinIO)
// - S3_BUCKET: S3 bucket name for testing
//
// If these environment variables are not set, the test will be skipped.

func TestS3RegionStorage(t *testing.T) {
	re := require.New(t)
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		fmt.Println(awsAccessKey, awsSecretKey, awsRegion, s3Endpoint, s3Bucket)
		t.Skip("Skipping S3 region storage test: AWS credentials or S3 configuration not provided")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create S3 config
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-pd-regions/",
		Endpoint: s3Endpoint,
	}

	// Create S3 region storage without encryption for now
	regionStorage, err := NewRegionStorageWithS3Backend(ctx, s3Config, nil)
	re.NoError(err)
	defer regionStorage.Close()

	// Flush any pending writes to ensure clean state
	err = regionStorage.Flush()
	re.NoError(err)

	// Test basic operations
	region1 := newTestRegionMeta(1)
	err = regionStorage.SaveRegion(region1)
	re.NoError(err)

	region2 := newTestRegionMeta(2)
	err = regionStorage.SaveRegion(region2)
	re.NoError(err)

	// Flush to ensure data is written to S3
	err = regionStorage.Flush()
	re.NoError(err)

	// Load regions
	regions := make([]*core.RegionInfo, 0)
	appendRegionFunc := func(region *core.RegionInfo) []*core.RegionInfo {
		regions = append(regions, region)
		return nil
	}
	err = regionStorage.LoadRegions(ctx, appendRegionFunc)
	re.NoError(err)
	re.Len(regions, 2)

	// Load individual region
	loadedRegion := &metapb.Region{}
	ok, err := regionStorage.LoadRegion(1, loadedRegion)
	re.NoError(err)
	re.True(ok)
	re.Equal(region1.Id, loadedRegion.Id)

	// Delete region
	err = regionStorage.DeleteRegion(region1)
	re.NoError(err)

	// Flush delete operation
	err = regionStorage.Flush()
	re.NoError(err)

	// Verify deletion
	ok, err = regionStorage.LoadRegion(1, loadedRegion)
	re.NoError(err)
	re.False(ok)

	// Verify remaining region still exists
	ok, err = regionStorage.LoadRegion(2, loadedRegion)
	re.NoError(err)
	re.True(ok)
	re.Equal(region2.Id, loadedRegion.Id)
}

// setupS3Storage creates an S3 storage for benchmarking
func setupS3Storage(b *testing.B) endpoint.RegionStorage {
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		b.Skip("Skipping S3 benchmark: AWS credentials or S3 configuration not provided")
	}

	ctx := context.Background()
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   fmt.Sprintf("benchmark-%s/", b.Name()),
		Endpoint: s3Endpoint,
	}

	storage, err := NewRegionStorageWithS3Backend(ctx, s3Config, nil)
	if err != nil {
		b.Fatalf("Failed to create S3 storage: %v", err)
	}

	return storage
}

// BenchmarkRegionStorageWrite benchmarks write operations for both LevelDB and S3
func BenchmarkRegionStorageWrite(b *testing.B) {
	benchmarkCases := []struct {
		name      string
		setupFn   func(b *testing.B) endpoint.RegionStorage
		batchSize int
	}{
		{
			name: "LevelDB",
			setupFn: func(*testing.B) endpoint.RegionStorage {
				return NewStorageWithMemoryBackend()
			},
			batchSize: 100,
		},
		{
			name:      "S3",
			setupFn:   setupS3Storage,
			batchSize: 1000,
		},
	}

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			storage := bc.setupFn(b)
			defer storage.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				regionID := uint64(0)
				for pb.Next() {
					regionID++
					region := newTestRegionMeta(regionID)
					err := storage.SaveRegion(region)
					if err != nil {
						b.Fatalf("Failed to save region: %v", err)
					}

					// Flush every batchSize operations
					if regionID%uint64(bc.batchSize) == 0 {
						if flusher, ok := storage.(interface{ Flush() error }); ok {
							err := flusher.Flush()
							if err != nil {
								b.Fatalf("Failed to flush: %v", err)
							}
						}
					}
				}
			})
		})
	}
}

// BenchmarkRegionStorageRead benchmarks read operations for both LevelDB and S3
func BenchmarkRegionStorageRead(b *testing.B) {
	benchmarkCases := []struct {
		name     string
		setupFn  func(b *testing.B) endpoint.RegionStorage
		dataSize int
	}{
		{
			name: "LevelDB",
			setupFn: func(*testing.B) endpoint.RegionStorage {
				return NewStorageWithMemoryBackend()
			},
			dataSize: 1000,
		},
		{
			name:     "S3",
			setupFn:  setupS3Storage,
			dataSize: 1000,
		},
	}

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			storage := bc.setupFn(b)
			defer storage.Close()

			// Prepare test data
			for i := 1; i <= bc.dataSize; i++ {
				region := newTestRegionMeta(uint64(i))
				err := storage.SaveRegion(region)
				require.NoError(b, err)
			}

			// Flush all data
			if flusher, ok := storage.(interface{ Flush() error }); ok {
				err := flusher.Flush()
				require.NoError(b, err)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				regionID := uint64(1)
				for pb.Next() {
					loadedRegion := &metapb.Region{}
					_, err := storage.LoadRegion(regionID, loadedRegion)
					if err != nil {
						b.Fatalf("Failed to load region: %v", err)
					}

					regionID++
					if regionID > uint64(bc.dataSize) {
						regionID = 1
					}
				}
			})
		})
	}
}

// BenchmarkRegionStorageLoadAll benchmarks loading all regions for both LevelDB and S3
func BenchmarkRegionStorageLoadAll(b *testing.B) {
	benchmarkCases := []struct {
		name     string
		setupFn  func(b *testing.B) endpoint.RegionStorage
		dataSize int
	}{
		{
			name: "LevelDB",
			setupFn: func(*testing.B) endpoint.RegionStorage {
				return NewStorageWithMemoryBackend()
			},
			dataSize: 1000,
		},
		{
			name:     "S3",
			setupFn:  setupS3Storage,
			dataSize: 1000,
		},
	}

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			storage := bc.setupFn(b)
			defer storage.Close()

			// Prepare test data
			for i := 1; i <= bc.dataSize; i++ {
				region := newTestRegionMeta(uint64(i))
				err := storage.SaveRegion(region)
				require.NoError(b, err)
			}

			// Flush all data
			if flusher, ok := storage.(interface{ Flush() error }); ok {
				err := flusher.Flush()
				require.NoError(b, err)
			}

			b.ResetTimer()
			for range b.N {
				// Reset region ID for memory backend before each load operation
				if resetter, ok := storage.(interface{ ResetRegionID() }); ok {
					resetter.ResetRegionID()
				}

				ctx := context.Background()
				regions := make([]*core.RegionInfo, 0)
				appendRegionFunc := func(region *core.RegionInfo) []*core.RegionInfo {
					regions = append(regions, region)
					return nil
				}
				err := storage.LoadRegions(ctx, appendRegionFunc)
				if err != nil {
					b.Fatalf("Failed to load all regions: %v", err)
				}
				if len(regions) != bc.dataSize {
					b.Fatalf("Expected %d regions, got %d", bc.dataSize, len(regions))
				}
			}
		})
	}
}
