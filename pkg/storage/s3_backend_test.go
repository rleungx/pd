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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/kv"
)

var (
	awsAccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion    = os.Getenv("AWS_REGION")

	s3Endpoint = os.Getenv("S3_ENDPOINT")
	s3Bucket   = os.Getenv("S3_BUCKET")
)

// The following test requires S3 credentials and configuration to be set via environment variables:
// - AWS_ACCESS_KEY_ID: AWS access key
// - AWS_SECRET_ACCESS_KEY: AWS secret key
// - AWS_REGION: AWS region (optional, defaults to us-east-1)
// - S3_ENDPOINT: S3 endpoint URL (optional, for S3-compatible services like MinIO)
// - S3_BUCKET: S3 bucket name for testing
//
// If these environment variables are not set, the test will be skipped.

func TestS3Backend(t *testing.T) {
	re := require.New(t)

	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		t.Skip("Skipping S3 backend test: AWS credentials or S3 configuration not provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create S3 config
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-backend/",
		Endpoint: s3Endpoint,
	}

	// Create S3 backend
	backend, err := newS3Backend(ctx, s3Config, nil)
	re.NoError(err)
	defer backend.Close()

	// Test basic batch operations
	testS3BackendBatchOperations(re, backend)

	// Test flush operations
	testS3BackendFlush(re, backend)

	// Test background flush
	testS3BackendBackgroundFlush(re, backend)
}

func testS3BackendBatchOperations(re *require.Assertions, backend *s3Backend) {
	// Test SaveIntoBatch
	err := backend.SaveIntoBatch("key1", []byte("value1"))
	re.NoError(err)

	err = backend.SaveIntoBatch("key2", []byte("value2"))
	re.NoError(err)

	err = backend.SaveIntoBatch("key3", []byte("value3"))
	re.NoError(err)

	// Verify data is in batch but not yet flushed
	backend.mu.RLock()
	re.Equal(3, backend.cacheSize)
	re.Equal([]byte("value1"), backend.batch["key1"])
	re.Equal([]byte("value2"), backend.batch["key2"])
	re.Equal([]byte("value3"), backend.batch["key3"])
	backend.mu.RUnlock()

	// Manual flush to save data
	err = backend.Flush()
	re.NoError(err)

	// Verify batch is cleared after flush
	backend.mu.RLock()
	re.Equal(0, backend.cacheSize)
	re.Empty(backend.batch)
	backend.mu.RUnlock()

	// Verify data can be read back
	value, err := backend.Load("key1")
	re.NoError(err)
	re.Equal("value1", value)

	value, err = backend.Load("key2")
	re.NoError(err)
	re.Equal("value2", value)

	value, err = backend.Load("key3")
	re.NoError(err)
	re.Equal("value3", value)
}

func testS3BackendFlush(re *require.Assertions, backend *s3Backend) {
	// Test explicit flush
	err := backend.SaveIntoBatch("flush_key1", []byte("flush_value1"))
	re.NoError(err)

	err = backend.SaveIntoBatch("flush_key2", []byte("flush_value2"))
	re.NoError(err)

	// Verify data is in batch
	backend.mu.RLock()
	re.Equal(2, backend.cacheSize)
	backend.mu.RUnlock()

	// Flush data
	err = backend.Flush()
	re.NoError(err)

	// Verify batch is cleared
	backend.mu.RLock()
	re.Equal(0, backend.cacheSize)
	re.Empty(backend.batch)
	backend.mu.RUnlock()

	// Verify data is persisted
	value, err := backend.Load("flush_key1")
	re.NoError(err)
	re.Equal("flush_value1", value)

	value, err = backend.Load("flush_key2")
	re.NoError(err)
	re.Equal("flush_value2", value)
}

func testS3BackendBackgroundFlush(re *require.Assertions, backend *s3Backend) {
	// Add data to batch
	err := backend.SaveIntoBatch("bg_key1", []byte("bg_value1"))
	re.NoError(err)

	// Verify data is in batch initially
	backend.mu.RLock()
	initialCacheSize := backend.cacheSize
	backend.mu.RUnlock()
	re.Equal(1, initialCacheSize)

	// Manually flush to test flush functionality
	err = backend.Flush()
	re.NoError(err)

	// Verify batch is cleared after manual flush
	backend.mu.RLock()
	cacheSize := backend.cacheSize
	backend.mu.RUnlock()
	re.Equal(0, cacheSize)

	// Verify data is persisted
	value, err := backend.Load("bg_key1")
	re.NoError(err)
	re.Equal("bg_value1", value)
}

func TestS3BackendBatchSizeLimit(t *testing.T) {
	re := require.New(t)

	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		t.Skip("Skipping S3 backend test: AWS credentials or S3 configuration not provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create S3 config
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-batch-limit/",
		Endpoint: s3Endpoint,
	}

	// Create S3 backend
	backend, err := newS3Backend(ctx, s3Config, nil)
	re.NoError(err)
	defer backend.Close()

	// Set a small batch size for testing
	backend.batchSize = 3

	// Add data up to batch size limit
	err = backend.SaveIntoBatch("limit_key1", []byte("limit_value1"))
	re.NoError(err)

	err = backend.SaveIntoBatch("limit_key2", []byte("limit_value2"))
	re.NoError(err)

	// Verify data is still in batch
	backend.mu.RLock()
	re.Equal(2, backend.cacheSize)
	backend.mu.RUnlock()

	// Adding one more should trigger auto-flush due to batch size limit
	err = backend.SaveIntoBatch("limit_key3", []byte("limit_value3"))
	re.NoError(err)

	// Verify batch is cleared due to auto-flush
	backend.mu.RLock()
	re.Equal(0, backend.cacheSize)
	re.Empty(backend.batch)
	backend.mu.RUnlock()

	// Verify all data is persisted
	value, err := backend.Load("limit_key1")
	re.NoError(err)
	re.Equal("limit_value1", value)

	value, err = backend.Load("limit_key2")
	re.NoError(err)
	re.Equal("limit_value2", value)

	value, err = backend.Load("limit_key3")
	re.NoError(err)
	re.Equal("limit_value3", value)
}

func TestS3BackendClose(t *testing.T) {
	re := require.New(t)
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Bucket == "" || s3Endpoint == "" {
		t.Skip("Skipping S3 backend test: AWS credentials or S3 configuration not provided")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create S3 config
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-close/",
		Endpoint: s3Endpoint,
	}

	// Create S3 backend
	backend, err := newS3Backend(ctx, s3Config, nil)
	re.NoError(err)

	// Add some data to batch
	err = backend.SaveIntoBatch("close_key1", []byte("close_value1"))
	re.NoError(err)

	err = backend.SaveIntoBatch("close_key2", []byte("close_value2"))
	re.NoError(err)

	// Verify data is in batch
	backend.mu.RLock()
	re.Equal(2, backend.cacheSize)
	backend.mu.RUnlock()

	// Close should flush remaining data
	err = backend.Close()
	re.NoError(err)

	// Create a new backend to verify data was persisted
	newBackend, err := newS3Backend(ctx, s3Config, nil)
	re.NoError(err)
	defer newBackend.Close()

	// Verify data is persisted
	value, err := newBackend.Load("close_key1")
	re.NoError(err)
	re.Equal("close_value1", value)

	value, err = newBackend.Load("close_key2")
	re.NoError(err)
	re.Equal("close_value2", value)
}

// BenchmarkS3BackendBatch benchmarks the batch operations of S3 backend
func BenchmarkS3BackendBatch(b *testing.B) {
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		b.Skip("Skipping S3 backend benchmark: AWS credentials or S3 configuration not provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "bench-batch/",
		Endpoint: s3Endpoint,
	}

	backend, err := newS3Backend(ctx, s3Config, nil)
	if err != nil {
		b.Fatalf("Failed to create S3 backend: %v", err)
	}
	defer backend.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "bench_key_" + string(rune(i))
			value := "bench_value_" + string(rune(i))
			err := backend.SaveIntoBatch(key, []byte(value))
			if err != nil {
				b.Fatalf("Failed to save into batch: %v", err)
			}
			i++
		}
	})
}

// TestS3BackendDeduplication tests that the S3 backend properly deduplicates data.
func TestS3BackendDeduplication(t *testing.T) {
	re := require.New(t)

	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		t.Skip("Skipping S3 backend deduplication test: AWS credentials or S3 configuration not provided")
	}

	ctx := context.Background()
	s3Config := &kv.S3Config{
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-dedup/",
		Endpoint: s3Endpoint,
	}

	backend, err := newS3Backend(ctx, s3Config, nil)
	re.NoError(err)
	defer backend.Close()

	// Test data
	key := "test-region-123"
	value1 := []byte("region-data-v1")
	value2 := []byte("region-data-v2")

	// Save same data twice - should be deduplicated
	err = backend.SaveIntoBatch(key, value1)
	re.NoError(err)
	initialCacheSize := backend.cacheSize

	// Save same data again - should not increase cache size
	err = backend.SaveIntoBatch(key, value1)
	re.NoError(err)
	re.Equal(initialCacheSize, backend.cacheSize, "Cache size should not increase for duplicate data")

	// Save different data - should increase cache size or update existing
	err = backend.SaveIntoBatch(key, value2)
	re.NoError(err)
	re.Equal(initialCacheSize, backend.cacheSize, "Cache size should remain same when updating existing key")

	// Verify the latest value is stored
	re.Equal(value2, backend.batch[key], "Latest value should be stored in batch")

	// Save to different key - should increase cache size
	err = backend.SaveIntoBatch("test-region-456", value1)
	re.NoError(err)
	re.Equal(initialCacheSize+1, backend.cacheSize, "Cache size should increase for new key")

	// Flush and verify
	err = backend.Flush()
	re.NoError(err)
	re.Equal(0, backend.cacheSize, "Cache should be empty after flush")

	value, err := backend.Load(key)
	re.NoError(err)
	re.Equal(string(value2), value)
}
