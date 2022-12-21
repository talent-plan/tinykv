// Copyright 2018 PingCAP, Inc.
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

package ycsb

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
)

// WorkloadCreator creates a Workload
type WorkloadCreator interface {
	Create(p *properties.Properties) (Workload, error)
}

// Workload defines different workload for YCSB.
type Workload interface {
	// Close closes the workload.
	Close() error

	// InitThread initializes the state associated to the goroutine worker.
	// The Returned context will be passed to the following DoInsert and DoTransaction.
	InitThread(ctx context.Context, threadID int, threadCount int) context.Context

	// CleanupThread cleans up the state when the worker finished.
	CleanupThread(ctx context.Context)

	// Load data into DB.
	Load(ctx context.Context, db DB, totalCount int64) error

	// DoInsert does one insert operation.
	DoInsert(ctx context.Context, db DB) error

	// DoBatchInsert does batch insert.
	DoBatchInsert(ctx context.Context, batchSize int, db DB) error

	// DoTransaction does one transaction operation.
	DoTransaction(ctx context.Context, db DB) error

	// DoBatchTransaction does the batch transaction operation.
	DoBatchTransaction(ctx context.Context, batchSize int, db DB) error
}

var workloadCreators = map[string]WorkloadCreator{}

// RegisterWorkloadCreator registers a creator for the workload
func RegisterWorkloadCreator(name string, creator WorkloadCreator) {
	_, ok := workloadCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register workload %s", name))
	}

	workloadCreators[name] = creator
}

// GetWorkloadCreator gets the WorkloadCreator for the database
func GetWorkloadCreator(name string) WorkloadCreator {
	return workloadCreators[name]
}
