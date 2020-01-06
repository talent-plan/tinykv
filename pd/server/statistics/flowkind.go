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

package statistics

// FlowKind is a identify Flow types.
type FlowKind uint32

// Flags for flow.
const (
	WriteFlow FlowKind = iota
	ReadFlow
)

func (k FlowKind) String() string {
	switch k {
	case WriteFlow:
		return "write"
	case ReadFlow:
		return "read"
	}
	return "unimplemented"
}
