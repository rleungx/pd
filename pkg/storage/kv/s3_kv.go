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

package kv

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	// defaultS3FlushRate is the default interval to flush the data to S3.
	defaultS3FlushRate = 2 * time.Second
	// bucketFileKey is the key for the single bucket file
	bucketFileKey = "regions"
)

var (
	awsAccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion    = os.Getenv("AWS_REGION")

	s3Endpoint = os.Getenv("S3_ENDPOINT")
	s3Bucket   = os.Getenv("S3_BUCKET")
)

// S3Config holds the configuration for S3 backend
type S3Config struct {
	Bucket   string
	Region   string
	Prefix   string
	Endpoint string // for S3-compatible services like MinIO
}

// bucketData represents a collection of regions stored in a single S3 object
type bucketData struct {
	Regions map[string][]byte `json:"regions"` // key -> value mapping
	Version int64             `json:"version"` // version for optimistic locking
	ETag    string            `json:"etag"`    // S3 ETag for conflict detection
}

// s3KV implements the Base interface for S3 storage
type s3KV struct {
	client    *s3.Client
	config    *S3Config
	mu        sync.RWMutex
	data      *bucketData
	dirty     bool      // whether the data has pending changes
	lastSync  time.Time // last time the data was synced to S3
	flushRate time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewS3KV creates a new S3 KV instance
func NewS3KV(ctx context.Context, s3Config *S3Config) (Base, error) {
	// Load AWS config with credentials from environment
	var cfg aws.Config
	var err error

	if awsAccessKey != "" && awsSecretKey != "" {
		// Use static credentials for MinIO
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s3Config.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				awsAccessKey, awsSecretKey, "",
			)),
		)
	} else {
		// Use default credential chain for AWS
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(s3Config.Region))
	}

	if err != nil {
		return nil, errors.Errorf("failed to load AWS config: %v", err)
	}

	var client *s3.Client
	if s3Config.Endpoint != "" {
		// Custom endpoint (e.g., MinIO)
		client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Config.Endpoint)
			o.UsePathStyle = true
		})
	} else {
		// Default AWS S3
		client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = false
		})
	}

	kvCtx, cancel := context.WithCancel(ctx)

	kv := &s3KV{
		client: client,
		config: s3Config,
		data: &bucketData{
			Regions: make(map[string][]byte),
		},
		flushRate: defaultS3FlushRate,
		ctx:       kvCtx,
		cancel:    cancel,
	}

	if err := kv.createBucketIfNotExists(); err != nil {
		return nil, errors.Errorf("failed to connect to S3: %v", err)
	}

	// Start background flush goroutine
	go kv.backgroundFlush()

	return kv, nil
}

// createBucketIfNotExists creates bucket if it doesn't exist
func (s *s3KV) createBucketIfNotExists() error {
	_, err := s.client.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.config.Bucket),
		Prefix:  aws.String(s.config.Prefix),
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		// Check if the error is "bucket not found"
		var noSuchBucket *types.NoSuchBucket
		if stderrors.As(err, &noSuchBucket) {
			// Bucket doesn't exist, try to create it
			log.Info("Bucket does not exist, creating it", zap.String("bucket", s.config.Bucket))

			_, createErr := s.client.CreateBucket(s.ctx, &s3.CreateBucketInput{
				Bucket: aws.String(s.config.Bucket),
			})
			if createErr != nil {
				return errors.Wrap(createErr, "failed to create S3 bucket")
			}

			log.Info("S3 bucket created successfully", zap.String("bucket", s.config.Bucket))
		} else {
			return errors.Wrap(err, "failed to connect to S3")
		}
	}

	return nil
}

// backgroundFlush periodically flushes dirty buckets to S3
func (s *s3KV) backgroundFlush() {
	ticker := time.NewTicker(s.flushRate)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Flush all dirty buckets
			_ = s.Flush()
		}
	}
}

// bucketKey returns the S3 key for the single bucket
func (s *s3KV) bucketKey() string {
	return fmt.Sprintf("%s%s", s.config.Prefix, bucketFileKey)
}

// Save saves a key-value pair
func (s *s3KV) Save(key, value string) error {
	return s.saveIntoBatch(key, []byte(value))
}

// saveIntoBatch saves a key-value pair to the single bucket
func (s *s3KV) saveIntoBatch(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data.Regions[key] = value
	s.dirty = true

	return nil
}

// Load loads a value by key
func (s *s3KV) Load(key string) (string, error) {
	s.mu.RLock()
	value, exists := s.data.Regions[key]
	s.mu.RUnlock()

	if !exists {
		// Try to load from S3
		if err := s.loadFromS3(); err != nil {
			return "", err
		}

		s.mu.RLock()
		value, exists = s.data.Regions[key]
		s.mu.RUnlock()

		if !exists {
			return "", nil // Key not found
		}
	}

	return string(value), nil
}

// Remove removes a key
func (s *s3KV) Remove(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data.Regions[key]; exists {
		delete(s.data.Regions, key)
		s.dirty = true
	}

	return nil
}

// LoadRange loads a range of key-value pairs
func (s *s3KV) LoadRange(startKey, endKey string, limit int) (keys []string, values []string, err error) {
	type keyValue struct {
		key   string
		value string
	}
	var allPairs []keyValue

	// Try to load from S3 if data is empty
	s.mu.RLock()
	if len(s.data.Regions) == 0 {
		s.mu.RUnlock()
		if err := s.loadFromS3(); err != nil {
			return nil, nil, err
		}
		s.mu.RLock()
	}

	for k, v := range s.data.Regions {
		if k >= startKey && (endKey == "" || k < endKey) {
			allPairs = append(allPairs, keyValue{key: k, value: string(v)})
		}
	}
	s.mu.RUnlock()

	// Sort by key
	sort.Slice(allPairs, func(i, j int) bool {
		return allPairs[i].key < allPairs[j].key
	})

	// Apply limit
	if limit > 0 && len(allPairs) > limit {
		allPairs = allPairs[:limit]
	}

	// Extract keys and values
	keys = make([]string, len(allPairs))
	values = make([]string, len(allPairs))
	for i, pair := range allPairs {
		keys[i] = pair.key
		values[i] = pair.value
	}

	return keys, values, nil
}

// loadFromS3 loads data from S3
func (s *s3KV) loadFromS3() error {
	bucketKey := s.bucketKey()

	result, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(bucketKey),
	})

	if err != nil {
		var noSuchKey *types.NoSuchKey
		if stderrors.As(err, &noSuchKey) {
			// File doesn't exist in S3, that's fine
			return nil
		}
		return errors.Wrapf(err, "failed to load data from S3")
	}
	defer result.Body.Close()

	// Read content from S3
	var buf []byte
	buf, err = io.ReadAll(result.Body)
	if err != nil {
		return errors.Wrapf(err, "failed to read data from S3")
	}

	if result.ETag != nil {
		log.Debug("S3 object loaded",
			zap.String("bucket", s.config.Bucket),
			zap.String("key", bucketKey),
			zap.String("etag", *result.ETag),
			zap.Int("size", len(buf)))
	}

	// Decode JSON
	var data bucketData
	if err := json.Unmarshal(buf, &data); err != nil {
		return errors.Wrapf(err, "failed to decode data")
	}

	// Store ETag for future conditional updates
	if result.ETag != nil {
		data.ETag = *result.ETag
	}

	s.mu.Lock()
	s.data = &data
	s.dirty = false
	s.lastSync = time.Now()
	s.mu.Unlock()

	return nil
}

// saveToS3 saves data to S3
func (s *s3KV) saveToS3(data *bucketData) error {
	bucketKey := s.bucketKey()

	// Encode to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "failed to encode data")
	}

	putInput := &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(bucketKey),
		Body:   strings.NewReader(string(jsonData)),
		Metadata: map[string]string{
			"version": strconv.FormatInt(data.Version, 10),
		},
	}

	// Upload to S3
	result, err := s.client.PutObject(s.ctx, putInput)
	if err != nil {
		return errors.Wrapf(err, "failed to save data to S3")
	}

	// Update ETag for future reference
	if result.ETag != nil {
		data.ETag = *result.ETag
	}

	return nil
}

// Flush flushes dirty data to S3
func (s *s3KV) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dirty {
		data := &bucketData{
			Regions: make(map[string][]byte),
			Version: s.data.Version + 1,
		}

		// Copy current data
		for k, v := range s.data.Regions {
			data.Regions[k] = v
		}

		if err := s.saveToS3(data); err != nil {
			return err
		}

		s.data = data
		s.dirty = false
		s.lastSync = time.Now()
	}

	return nil
}

// RunInTxn runs a function in a transaction
func (s *s3KV) RunInTxn(_ context.Context, f func(txn Txn) error) error {
	// S3 doesn't support traditional transactions, but we can batch operations
	txn := &s3Txn{s3KV: s, operations: make(map[string][]byte)}
	if err := f(txn); err != nil {
		return err
	}

	// Apply all operations atomically
	s.mu.Lock()
	for key, value := range txn.operations {
		if value == nil {
			delete(s.data.Regions, key)
		} else {
			s.data.Regions[key] = value
		}
	}
	s.dirty = true
	s.mu.Unlock()

	return s.Flush()
}

// CreateRawTxn creates a raw transaction (not supported for S3)
func (*s3KV) CreateRawTxn() RawTxn {
	panic("raw transactions not supported in S3 KV")
}

// s3Txn implements the Txn interface for S3
type s3Txn struct {
	s3KV       *s3KV
	operations map[string][]byte // key -> value, nil value means deletion
}

// Save saves a key-value pair in the transaction
func (txn *s3Txn) Save(key, value string) error {
	txn.operations[key] = []byte(value)
	return nil
}

// Remove removes a key in the transaction
func (txn *s3Txn) Remove(key string) error {
	txn.operations[key] = nil // Mark for deletion
	return nil
}

// Load loads a value by key in the transaction
func (txn *s3Txn) Load(key string) (string, error) {
	if value, exists := txn.operations[key]; exists {
		if value == nil {
			return "", nil // Key was deleted
		}
		return string(value), nil
	}
	return txn.s3KV.Load(key)
}

// LoadRange loads a range of key-value pairs in the transaction
func (txn *s3Txn) LoadRange(startKey, endKey string, limit int) (keys []string, values []string, err error) {
	// For simplicity, this doesn't account for transaction operations
	// In a full implementation, we would merge transaction ops with backend data
	return txn.s3KV.LoadRange(startKey, endKey, limit)
}

// Close closes the s3KV instance and stops background processes
func (s *s3KV) Close() error {
	// Cancel context to stop background goroutine
	if s.cancel != nil {
		s.cancel()
	}

	// Final flush to save any remaining data
	return s.Flush()
}
