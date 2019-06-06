// Copyright 2019 PingCAP, Inc.
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

package schedule

import (
	"math/rand"
	"time"
)

// PriorityWeight is used to represent the weight of different priorities of operators.
var PriorityWeight = []float64{1.0, 4.0, 9.0}

// WaitingOperator is an interface of waiting operators.
type WaitingOperator interface {
	PutOperator(op *Operator)
	GetOperator() []*Operator
}

// Bucket is used to maintain the operators created by a specific scheduler.
type Bucket struct {
	weight float64
	ops    []*Operator
}

// RandBuckets is an implementation of waiting operators
type RandBuckets struct {
	totalWeight float64
	buckets     []*Bucket
}

// NewRandBuckets creates a random buckets.
func NewRandBuckets() *RandBuckets {
	var buckets []*Bucket
	for i := 0; i < len(PriorityWeight); i++ {
		buckets = append(buckets, &Bucket{
			weight: PriorityWeight[i],
		})
	}
	return &RandBuckets{buckets: buckets}
}

// PutOperator puts an operator into the random buckets.
func (b *RandBuckets) PutOperator(op *Operator) {
	priority := op.GetPriorityLevel()
	bucket := b.buckets[priority]
	if len(bucket.ops) == 0 {
		b.totalWeight += bucket.weight
	}
	bucket.ops = append(bucket.ops, op)
}

// GetOperator gets an operator from the random buckets.
func (b *RandBuckets) GetOperator() []*Operator {
	if b.totalWeight == 0 {
		return nil
	}
	r := rand.Float64()
	var sum float64
	for _, bucket := range b.buckets {
		if len(bucket.ops) == 0 {
			continue
		}
		proportion := bucket.weight / b.totalWeight
		if r >= sum && r < sum+proportion {
			if len(bucket.ops) == 1 {
				b.totalWeight -= bucket.weight
			}
			var res []*Operator
			res = append(res, bucket.ops[0])
			// Merge operation has two operators, and thus it should be handled specifically.
			if bucket.ops[0].Desc() == "merge-region" {
				res = append(res, bucket.ops[1])
				bucket.ops = bucket.ops[2:]
			} else {
				bucket.ops = bucket.ops[1:]
			}
			return res
		}
		sum += proportion
	}
	return nil
}

// WaitingOperatorStatus is used to limit the count of each kind of operators.
type WaitingOperatorStatus struct {
	ops map[string]uint64
}

// NewWaitingOperatorStatus creates a new WaitingOperatorStatus.
func NewWaitingOperatorStatus() *WaitingOperatorStatus {
	return &WaitingOperatorStatus{
		make(map[string]uint64),
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
