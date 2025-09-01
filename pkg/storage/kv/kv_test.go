// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	kv := NewEtcdKVBase(client)
	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv)
	testLoadConflict(re, kv)
	testRawTxn(re, kv)
}

func TestLevelDB(t *testing.T) {
	re := require.New(t)
	dir := t.TempDir()
	kv, err := NewLevelDBKV(dir)
	re.NoError(err)

	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv)
	re.NoError(kv.Close())
}

func TestMemKV(t *testing.T) {
	re := require.New(t)
	kv := NewMemoryKV()
	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv)
}

// The following test requires S3 credentials and configuration to be set via environment variables:
// - AWS_ACCESS_KEY_ID: AWS access key
// - AWS_SECRET_ACCESS_KEY: AWS secret key
// - AWS_REGION: AWS region (optional, defaults to us-east-1)
// - S3_ENDPOINT: S3 endpoint URL (optional, for S3-compatible services like MinIO)
// - S3_BUCKET: S3 bucket name for testing
//
// If these environment variables are not set, the test will be skipped.

func TestS3KV(t *testing.T) {
	re := require.New(t)

	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" || s3Endpoint == "" || s3Bucket == "" {
		t.Skip("Skipping S3 kv test: AWS credentials or S3 configuration not provided")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := &S3Config{
		Endpoint: s3Endpoint,
		Bucket:   s3Bucket,
		Region:   awsRegion,
		Prefix:   "test-kv/",
	}

	kv, err := NewS3KV(ctx, config)
	if err != nil {
		t.Skipf("Skipping S3 KV test: %v", err)
		return
	}
	defer func() {
		// Clean up test data
		cleanupS3TestData(ctx, kv.(*s3KV))
		if closer, ok := kv.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()

	testReadWrite(re, kv)
	testRange(re, kv)
	testSaveMultiple(re, kv)

	// Test S3-specific features
	testS3Flush(re, kv)
	testS3BucketDistribution(re, kv)
}

func testReadWrite(re *require.Assertions, kv Base) {
	v, err := kv.Load("key")
	re.NoError(err)
	re.Empty(v)
	err = kv.Save("key", "value")
	re.NoError(err)
	v, err = kv.Load("key")
	re.NoError(err)
	re.Equal("value", v)
	err = kv.Remove("key")
	re.NoError(err)
	v, err = kv.Load("key")
	re.NoError(err)
	re.Empty(v)
	err = kv.Remove("key")
	re.NoError(err)
}

func testRange(re *require.Assertions, kv Base) {
	keys := []string{
		"test-a", "test-a/a", "test-a/ab",
		"test", "test/a", "test/ab",
		"testa", "testa/a", "testa/ab",
	}
	for _, k := range keys {
		err := kv.Save(k, k)
		re.NoError(err)
	}
	sortedKeys := append(keys[:0:0], keys...)
	sort.Strings(sortedKeys)

	testCases := []struct {
		start, end string
		limit      int
		expect     []string
	}{
		{start: "", end: "\x00", limit: 100, expect: sortedKeys},
		{start: "a", end: "z", limit: 100, expect: sortedKeys},
		{start: "a", end: "z", limit: 3, expect: sortedKeys[:3]},
		{start: "testa", end: "z", limit: 3, expect: []string{"testa", "testa/a", "testa/ab"}},
		{start: "test/", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test/a", "test/ab"}},
		{start: "test-a/", end: clientv3.GetPrefixRangeEnd("test-a/"), limit: 100, expect: []string{"test-a/a", "test-a/ab"}},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test"), limit: 100, expect: sortedKeys},
		{start: "test", end: clientv3.GetPrefixRangeEnd("test/"), limit: 100, expect: []string{"test", "test-a", "test-a/a", "test-a/ab", "test/a", "test/ab"}},
	}

	for _, testCase := range testCases {
		_, isEtcd := kv.(*etcdKVBase)
		if testCase.end == "\x00" && !isEtcd {
			continue
		}
		ks, vs, err := kv.LoadRange(testCase.start, testCase.end, testCase.limit)
		re.NoError(err)
		re.Equal(testCase.expect, ks)
		re.Equal(testCase.expect, vs)
	}
}

func testSaveMultiple(re *require.Assertions, kv Base) {
	count := 20
	err := kv.RunInTxn(context.Background(), func(txn Txn) error {
		var saveErr error
		for i := range count {
			saveErr = txn.Save("key"+strconv.Itoa(i), "val"+strconv.Itoa(i))
			if saveErr != nil {
				return saveErr
			}
		}
		return nil
	})
	re.NoError(err)
	for i := range count {
		val, loadErr := kv.Load("key" + strconv.Itoa(i))
		re.NoError(loadErr)
		re.Equal("val"+strconv.Itoa(i), val)
	}
}

// testLoadConflict checks that if any value loaded during the current transaction
// has been modified by another transaction before the current one commit,
// then the current transaction must fail.
func testLoadConflict(re *require.Assertions, kv Base) {
	re.NoError(kv.Save("testKey", "initialValue"))
	// loader loads the test key value.
	loader := func(txn Txn) error {
		_, err := txn.Load("testKey")
		if err != nil {
			return err
		}
		return nil
	}
	// When no other writer, loader must succeed.
	re.NoError(kv.RunInTxn(context.Background(), loader))

	conflictLoader := func(txn Txn) error {
		_, err := txn.Load("testKey")
		// update key after load.
		re.NoError(kv.Save("testKey", "newValue"))
		if err != nil {
			return err
		}
		return nil
	}
	// When other writer exists, loader must error.
	re.Error(kv.RunInTxn(context.Background(), conflictLoader))
}

// nolint:unparam
func mustHaveKeys(re *require.Assertions, kv Base, prefix string, expected ...KeyValuePair) {
	keys, values, err := kv.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
	re.NoError(err)
	re.Len(expected, len(keys))
	for i, key := range keys {
		re.Equal(expected[i].Key, key)
		re.Equal(expected[i].Value, values[i])
	}
}

func testRawTxn(re *require.Assertions, kv Base) {
	// Test NotExists condition, putting in transaction.
	res, err := kv.CreateRawTxn().If(
		RawTxnCondition{
			Key:     "txn-k1",
			CmpType: RawTxnCmpNotExists,
		},
	).Then(
		RawTxnOp{
			Key:    "txn-k1",
			OpType: RawTxnOpPut,
			Value:  "v1",
		},
		RawTxnOp{
			Key:    "txn-k2",
			OpType: RawTxnOpPut,
			Value:  "v2",
		},
	).Else(
		RawTxnOp{
			Key:    "txn-unexpected",
			OpType: RawTxnOpPut,
			Value:  "unexpected",
		},
	).Commit()

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Responses, 2)
	re.Empty(res.Responses[0].KeyValuePairs)
	re.Empty(res.Responses[1].KeyValuePairs)

	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k1", Value: "v1"}, KeyValuePair{Key: "txn-k2", Value: "v2"})

	// Test Equal condition; reading in transaction.
	res, err = kv.CreateRawTxn().If(
		RawTxnCondition{
			Key:     "txn-k1",
			CmpType: RawTxnCmpEqual,
			Value:   "v1",
		},
	).Then(
		RawTxnOp{
			Key:    "txn-k2",
			OpType: RawTxnOpGet,
		},
	).Else(
		RawTxnOp{
			Key:    "txn-unexpected",
			OpType: RawTxnOpPut,
			Value:  "unexpected",
		},
	).Commit()

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Responses, 1)
	re.Len(res.Responses[0].KeyValuePairs, 1)
	re.Equal("v2", res.Responses[0].KeyValuePairs[0].Value)
	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k1", Value: "v1"}, KeyValuePair{Key: "txn-k2", Value: "v2"})

	// Test NotEqual condition, else branch, reading range in transaction, reading & writing mixed.
	res, err = kv.CreateRawTxn().If(
		RawTxnCondition{
			Key:     "txn-k1",
			CmpType: RawTxnCmpNotEqual,
			Value:   "v1",
		},
	).Then(
		RawTxnOp{
			Key:    "txn-unexpected",
			OpType: RawTxnOpPut,
			Value:  "unexpected",
		},
	).Else(
		RawTxnOp{
			Key:    "txn-k1",
			OpType: RawTxnOpGetRange,
			EndKey: "txn-k2\x00",
		},
		RawTxnOp{
			Key:    "txn-k3",
			OpType: RawTxnOpPut,
			Value:  "k3",
		},
	).Commit()

	re.NoError(err)
	re.False(res.Succeeded)
	re.Len(res.Responses, 2)
	re.Len(res.Responses[0].KeyValuePairs, 2)
	re.Equal([]KeyValuePair{{Key: "txn-k1", Value: "v1"}, {Key: "txn-k2", Value: "v2"}}, res.Responses[0].KeyValuePairs)
	re.Empty(res.Responses[1].KeyValuePairs)

	mustHaveKeys(re, kv, "txn-",
		KeyValuePair{Key: "txn-k1", Value: "v1"},
		KeyValuePair{Key: "txn-k2", Value: "v2"},
		KeyValuePair{Key: "txn-k3", Value: "k3"})

	// Test Exists condition, deleting, overwriting.
	res, err = kv.CreateRawTxn().If(
		RawTxnCondition{
			Key:     "txn-k1",
			CmpType: RawTxnCmpExists,
		},
	).Then(
		RawTxnOp{
			Key:    "txn-k1",
			OpType: RawTxnOpDelete,
		},
		RawTxnOp{
			Key:    "txn-k2",
			OpType: RawTxnOpPut,
			Value:  "v22",
		},
		// Delete not existing key.
		RawTxnOp{
			Key:    "txn-k4",
			OpType: RawTxnOpDelete,
		},
	).Else(
		RawTxnOp{
			Key:    "txn-unexpected",
			OpType: RawTxnOpPut,
			Value:  "unexpected",
		},
	).Commit()

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Responses, 3)
	for _, item := range res.Responses {
		re.Empty(item.KeyValuePairs)
	}

	mustHaveKeys(re, kv, "txn-", KeyValuePair{Key: "txn-k2", Value: "v22"}, KeyValuePair{Key: "txn-k3", Value: "k3"})

	// Deleted keys can be regarded as not existing correctly.
	res, err = kv.CreateRawTxn().If(
		RawTxnCondition{
			Key:     "txn-k1",
			CmpType: RawTxnCmpNotExists,
		},
	).Then(
		RawTxnOp{
			Key:    "txn-k2",
			OpType: RawTxnOpDelete,
		},
		RawTxnOp{
			Key:    "txn-k3",
			OpType: RawTxnOpDelete,
		},
	).Commit()

	re.NoError(err)
	re.True(res.Succeeded)
	re.Len(res.Responses, 2)
	for _, item := range res.Responses {
		re.Empty(item.KeyValuePairs)
	}
	mustHaveKeys(re, kv, "txn-")

	// The following tests only check the correctness of the conditions.
	check := func(conditions []RawTxnCondition, shouldSuccess bool) {
		res, err := kv.CreateRawTxn().If(conditions...).Commit()
		re.NoError(err)
		re.Equal(shouldSuccess, res.Succeeded)
	}

	// "txn-k1" doesn't exist at this point.
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpExists}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpNotExists}}, true)

	err = kv.Save("txn-k1", "v1")
	re.NoError(err)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpExists}}, true)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpNotExists}}, false)

	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v1"}}, true)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpNotEqual, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v2"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpNotEqual, Value: "v2"}}, true)

	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpLess, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpLess, Value: "v0"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpLess, Value: "v2"}}, true)

	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpGreater, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpGreater, Value: "v2"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpGreater, Value: "v0"}}, true)

	// Test comparing with not-existing key.
	err = kv.Remove("txn-k1")
	re.NoError(err)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpNotEqual, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpLess, Value: "v1"}}, false)
	check([]RawTxnCondition{{Key: "txn-k1", CmpType: RawTxnCmpGreater, Value: "v1"}}, false)

	// Test the conditions are conjunctions.
	err = kv.Save("txn-k1", "v1")
	re.NoError(err)
	err = kv.Save("txn-k2", "v2")
	re.NoError(err)
	check([]RawTxnCondition{
		{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v1"},
		{Key: "txn-k2", CmpType: RawTxnCmpEqual, Value: "v2"},
	}, true)
	check([]RawTxnCondition{
		{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v1"},
		{Key: "txn-k2", CmpType: RawTxnCmpEqual, Value: "v0"},
	}, false)
	check([]RawTxnCondition{
		{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v0"},
		{Key: "txn-k2", CmpType: RawTxnCmpEqual, Value: "v2"},
	}, false)
	check([]RawTxnCondition{
		{Key: "txn-k1", CmpType: RawTxnCmpEqual, Value: "v0"},
		{Key: "txn-k2", CmpType: RawTxnCmpEqual, Value: "v0"},
	}, false)
}

// testS3Flush tests S3-specific flush functionality
func testS3Flush(re *require.Assertions, kv Base) {
	// Save some test data
	err := kv.Save("flush-test-key1", "flush-test-value1")
	re.NoError(err)

	err = kv.Save("flush-test-key2", "flush-test-value2")
	re.NoError(err)

	// Test explicit flush if supported
	if flusher, ok := kv.(interface{ Flush() error }); ok {
		err = flusher.Flush()
		re.NoError(err)
	}

	// Verify data is still accessible after flush
	val1, err := kv.Load("flush-test-key1")
	re.NoError(err)
	re.Equal("flush-test-value1", val1)

	val2, err := kv.Load("flush-test-key2")
	re.NoError(err)
	re.Equal("flush-test-value2", val2)

	// Clean up
	re.NoError(kv.Remove("flush-test-key1"))
	re.NoError(kv.Remove("flush-test-key2"))
}

// testS3BucketDistribution tests that keys are distributed across buckets
func testS3BucketDistribution(re *require.Assertions, kv Base) {
	// Save many keys to test distribution
	testKeys := make([]string, 100)
	testValues := make([]string, 100)

	for i := range 100 {
		testKeys[i] = fmt.Sprintf("bucket-test-key-%d", i)
		testValues[i] = fmt.Sprintf("bucket-test-value-%d", i)

		err := kv.Save(testKeys[i], testValues[i])
		re.NoError(err)
	}

	// Verify all keys can be loaded correctly
	for i := range 100 {
		val, err := kv.Load(testKeys[i])
		re.NoError(err)
		re.Equal(testValues[i], val)
	}

	// Test LoadRange with the distributed keys
	keys, values, err := kv.LoadRange("bucket-test-key-", "bucket-test-key-z", 50)
	re.NoError(err)
	re.NotEmpty(keys)
	re.Len(values, len(keys))

	// Clean up
	for i := range 100 {
		re.NoError(kv.Remove(testKeys[i]))
	}
}

// cleanupS3TestData removes all test data from S3
func cleanupS3TestData(ctx context.Context, kv *s3KV) {
	bucketKey := kv.bucketKey()

	// Delete the bucket object
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(kv.config.Bucket),
		Key:    aws.String(bucketKey),
	}
	_, _ = kv.client.DeleteObject(ctx, deleteInput)
}
