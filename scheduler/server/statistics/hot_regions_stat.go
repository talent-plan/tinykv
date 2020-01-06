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

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalBytesRate float64       `json:"total_flow_bytes"`
	RegionsCount   int           `json:"regions_count"`
	RegionsStat    []HotPeerStat `json:"statistics"`
}
