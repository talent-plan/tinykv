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

// StoreHotRegionInfos : used to get human readable description for hot regions.
type StoreHotRegionInfos struct {
	AsPeer   StoreHotRegionsStat `json:"as_peer"`
	AsLeader StoreHotRegionsStat `json:"as_leader"`
}

// StoreHotRegionsStat used to record the hot region statistics group by store
type StoreHotRegionsStat map[uint64]*HotRegionsStat
