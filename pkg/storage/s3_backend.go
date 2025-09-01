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
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// defaultS3FlushRate is the default interval to flush the data to S3.
	defaultS3FlushRate = 2 * time.Second
	// defaultS3BatchSize is the default batch size to save the data to S3.
	defaultS3BatchSize = 1000
)

// s3Backend is a storage backend that stores data in AWS S3 or S3-compatible storage,
// which is mainly used to store the PD Region meta information.
type s3Backend struct {
	*endpoint.StorageEndpoint
	ekm       *encryption.Manager
	mu        syncutil.RWMutex
	batch     map[string][]byte
	batchSize int
	cacheSize int
	flushRate time.Duration
	flushTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup // To wait for background goroutine to finish
}

// newS3Backend creates a new S3 backend.
func newS3Backend(
	ctx context.Context,
	s3Config *kv.S3Config,
	ekm *encryption.Manager,
) (*s3Backend, error) {
	// Create S3 KV instance
	s3KV, err := kv.NewS3KV(ctx, s3Config)
	if err != nil {
		return nil, err
	}

	// Create storage endpoint with S3 KV
	storageEndpoint := endpoint.NewStorageEndpoint(s3KV, ekm)

	sbCtx, cancel := context.WithCancel(ctx)

	sb := &s3Backend{
		StorageEndpoint: storageEndpoint,
		ekm:             ekm,
		batch:           make(map[string][]byte, defaultS3BatchSize),
		batchSize:       defaultS3BatchSize,
		flushRate:       defaultS3FlushRate,
		ctx:             sbCtx,
		cancel:          cancel,
	}

	// Start background flush goroutine
	sb.wg.Add(1)
	go sb.backgroundFlush()

	return sb, nil
}

// backgroundFlush periodically flushes dirty batch to S3
func (sb *s3Backend) backgroundFlush() {
	defer sb.wg.Done()
	ticker := time.NewTicker(sb.flushRate)
	defer ticker.Stop()

	for {
		select {
		case <-sb.ctx.Done():
			return
		case <-ticker.C:
			sb.mu.Lock()
			if time.Now().After(sb.flushTime) && sb.cacheSize > 0 {
				_ = sb.flushLocked()
			}
			sb.mu.Unlock()
		}
	}
}

// SaveIntoBatch saves the key-value pair into the batch cache, and it will
// only be saved to the underlying storage when the `Flush` method is
// called or the cache is full. It performs deduplication by checking if the
// value has changed before adding to batch.
func (sb *s3Backend) SaveIntoBatch(key string, value []byte) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Check if the value has changed (deduplication)
	if existingValue, exists := sb.batch[key]; exists && bytes.Equal(existingValue, value) {
		// Value hasn't changed, no need to update
		return nil
	}

	// Only increment cacheSize for new keys
	isNewKey := false
	if _, exists := sb.batch[key]; !exists {
		isNewKey = true
	}

	sb.batch[key] = value
	if isNewKey {
		sb.cacheSize++
	}

	if sb.cacheSize >= sb.batchSize {
		return sb.flushLocked()
	}

	sb.flushTime = time.Now().Add(sb.flushRate)
	return nil
}

// Flush saves the batch cache to the underlying storage.
func (sb *s3Backend) Flush() error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.flushLocked()
}

func (sb *s3Backend) flushLocked() error {
	if err := sb.saveBatchLocked(); err != nil {
		return err
	}
	sb.cacheSize = 0
	sb.batch = make(map[string][]byte, sb.batchSize)
	return nil
}

func (sb *s3Backend) saveBatchLocked() error {
	return sb.RunInTxn(sb.ctx, func(txn kv.Txn) error {
		for key, value := range sb.batch {
			if err := txn.Save(key, string(value)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close will gracefully close the S3 backend and flush the data to the underlying storage before closing.
func (sb *s3Backend) Close() error {
	// Cancel the context to stop background goroutine
	sb.cancel()

	// Wait for background goroutine to finish
	sb.wg.Wait()

	// Final flush to save any remaining data
	err := sb.Flush()
	if err != nil {
		return err
	}

	// Close the underlying KV storage
	if closer, ok := sb.Base.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
