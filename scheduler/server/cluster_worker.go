// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	if err := c.processRegionHeartbeat(region); err != nil {
		return err
	}

	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warn("invalid region, zero region peer count", zap.Stringer("region-meta", core.RegionToHexMeta(region.GetMeta())))
		return errors.Errorf("invalid region, zero region peer count: %v", core.RegionToHexMeta(region.GetMeta()))
	}

	c.RLock()
	co := c.coordinator
	c.RUnlock()
	co.opController.Dispatch(region, schedule.DispatchFromHeartBeat)
	return nil
}

func (c *RaftCluster) handleAskSplit(request *schedulerpb.AskSplitRequest) (*schedulerpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	err := c.validRequestRegion(reqRegion)
	if err != nil {
		return nil, err
	}

	newRegionID, err := c.s.idAllocator.Alloc()
	if err != nil {
		return nil, err
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.s.idAllocator.Alloc(); err != nil {
			return nil, err
		}
	}

	split := &schedulerpb.AskSplitResponse{
		NewRegionId: newRegionID,
		NewPeerIds:  peerIDs,
	}

	return split, nil
}

func (c *RaftCluster) validRequestRegion(reqRegion *metapb.Region) error {
	startKey := reqRegion.GetStartKey()
	region, _ := c.GetRegionByKey(startKey)
	if region == nil {
		return errors.Errorf("region not found, request region: %v", core.RegionToHexMeta(reqRegion))
	}
	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	}
	return nil
}

func (c *RaftCluster) checkSplitRegion(left *metapb.Region, right *metapb.Region) error {
	if left == nil || right == nil {
		return errors.New("invalid split region")
	}

	if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
		return errors.New("invalid split region")
	}

	if len(right.GetEndKey()) == 0 || bytes.Compare(left.GetStartKey(), right.GetEndKey()) < 0 {
		return nil
	}

	return errors.New("invalid split region")
}

func (c *RaftCluster) checkSplitRegions(regions []*metapb.Region) error {
	if len(regions) <= 1 {
		return errors.New("invalid split region")
	}

	for i := 1; i < len(regions); i++ {
		left := regions[i-1]
		right := regions[i]
		if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
			return errors.New("invalid split region")
		}
		if len(right.GetEndKey()) != 0 && bytes.Compare(left.GetStartKey(), right.GetEndKey()) >= 0 {
			return errors.New("invalid split region")
		}
	}
	return nil
}

func (c *RaftCluster) handleReportSplit(request *schedulerpb.ReportSplitRequest) (*schedulerpb.ReportSplitResponse, error) {
	left := request.GetLeft()
	right := request.GetRight()

	err := c.checkSplitRegion(left, right)
	if err != nil {
		log.Warn("report split region is invalid",
			zap.Stringer("left-region", core.RegionToHexMeta(left)),
			zap.Stringer("right-region", core.RegionToHexMeta(right)),
			zap.Error(err))
		return nil, err
	}

	// Build origin region by using left and right.
	originRegion := proto.Clone(right).(*metapb.Region)
	originRegion.RegionEpoch = nil
	originRegion.StartKey = left.GetStartKey()
	log.Info("region split, generate new region",
		zap.Uint64("region-id", originRegion.GetId()),
		zap.Stringer("region-meta", core.RegionToHexMeta(left)))
	return &schedulerpb.ReportSplitResponse{}, nil
}
