// Copyright 2017 PingCAP, Inc.
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

package core

// PriorityLevel lower level means higher priority
type PriorityLevel int

// Built-in priority level
const (
	LowPriority PriorityLevel = iota
	NormalPriority
	HighPriority
)

// ScheduleKind distinguishes resources and schedule strategy.
type ScheduleKind struct {
	Resource ResourceKind
	Strategy ScheduleStrategy
}

// NewScheduleKind creates a schedule kind with resource kind and schedule strategy.
func NewScheduleKind(Resource ResourceKind, Strategy ScheduleStrategy) ScheduleKind {
	return ScheduleKind{
		Resource: Resource,
		Strategy: Strategy,
	}
}

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// LeaderKind indicates the leader kind resource
	LeaderKind ResourceKind = iota
	// RegionKind indicates the region kind resource
	RegionKind
)

func (k ResourceKind) String() string {
	switch k {
	case LeaderKind:
		return "leader"
	case RegionKind:
		return "region"
	default:
		return "unknown"
	}
}

// ScheduleStrategy distinguishes different kinds of schedule strategy
type ScheduleStrategy int

const (
	// ByCount indicates that balance by count
	ByCount ScheduleStrategy = iota
	// BySize indicates that balance by size
	BySize
)

func (k ScheduleStrategy) String() string {
	switch k {
	case ByCount:
		return "count"
	case BySize:
		return "size"
	default:
		return "unknown"
	}
}

// StringToScheduleStrategy creates a schedule strategy with string.
func StringToScheduleStrategy(input string) ScheduleStrategy {
	switch input {
	case BySize.String():
		return BySize
	case ByCount.String():
		return ByCount
	default:
		panic("invalid schedule strategy: " + input)
	}
}

// KeyType distinguishes different kinds of key types
type KeyType int

const (
	// Table indicates that the key is table key
	Table KeyType = iota
	// Raw indicates that the key is raw key.
	Raw
	// Txn indicates that the key is txn key.
	Txn
)

func (k KeyType) String() string {
	switch k {
	case Table:
		return "table"
	case Raw:
		return "raw"
	case Txn:
		return "txn"
	default:
		return "unknown"
	}
}

// StringToKeyType creates a key type with string.
func StringToKeyType(input string) KeyType {
	switch input {
	case Table.String():
		return Table
	case Raw.String():
		return Raw
	case Txn.String():
		return Txn
	default:
		panic("invalid key type: " + input)
	}
}
