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

package server

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const heartbeatStreamKeepAliveInterval = time.Minute

type heartbeatStream interface {
	Send(*schedulerpb.RegionHeartbeatResponse) error
}

type streamUpdate struct {
	storeID uint64
	stream  heartbeatStream
}

type heartbeatStreams struct {
	wg             sync.WaitGroup
	hbStreamCtx    context.Context
	hbStreamCancel context.CancelFunc
	clusterID      uint64
	streams        map[uint64]heartbeatStream
	msgCh          chan *schedulerpb.RegionHeartbeatResponse
	streamCh       chan streamUpdate
	cluster        *RaftCluster
}

func newHeartbeatStreams(ctx context.Context, clusterID uint64, cluster *RaftCluster) *heartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &heartbeatStreams{
		hbStreamCtx:    hbStreamCtx,
		hbStreamCancel: hbStreamCancel,
		clusterID:      clusterID,
		streams:        make(map[uint64]heartbeatStream),
		msgCh:          make(chan *schedulerpb.RegionHeartbeatResponse, regionheartbeatSendChanCap),
		streamCh:       make(chan streamUpdate, 1),
		cluster:        cluster,
	}
	hs.wg.Add(1)
	go hs.run()
	return hs
}

func (s *heartbeatStreams) run() {
	defer logutil.LogPanic()

	defer s.wg.Done()

	keepAliveTicker := time.NewTicker(heartbeatStreamKeepAliveInterval)
	defer keepAliveTicker.Stop()

	keepAlive := &schedulerpb.RegionHeartbeatResponse{Header: &schedulerpb.ResponseHeader{ClusterId: s.clusterID}}

	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.storeID] = update.stream
		case msg := <-s.msgCh:
			storeID := msg.GetTargetPeer().GetStoreId()
			store := s.cluster.GetStore(storeID)
			if store == nil {
				log.Error("failed to get store",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID))
				delete(s.streams, storeID)
				continue
			}
			if stream, ok := s.streams[storeID]; ok {
				if err := stream.Send(msg); err != nil {
					log.Error("send heartbeat message fail",
						zap.Uint64("region-id", msg.RegionId), zap.Error(err))
					delete(s.streams, storeID)
				}
			} else {
				log.Debug("heartbeat stream not found, skip send message",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID))
			}
		case <-keepAliveTicker.C:
			for storeID, stream := range s.streams {
				store := s.cluster.GetStore(storeID)
				if store == nil {
					log.Error("failed to get store", zap.Uint64("store-id", storeID))
					delete(s.streams, storeID)
					continue
				}
				if err := stream.Send(keepAlive); err != nil {
					log.Error("send keepalive message fail",
						zap.Uint64("target-store-id", storeID),
						zap.Error(err))
					delete(s.streams, storeID)
				}
			}
		case <-s.hbStreamCtx.Done():
			return
		}
	}
}

func (s *heartbeatStreams) Close() {
	s.hbStreamCancel()
	s.wg.Wait()
}

func (s *heartbeatStreams) bindStream(storeID uint64, stream heartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.hbStreamCtx.Done():
	}
}

func (s *heartbeatStreams) SendMsg(region *core.RegionInfo, msg *schedulerpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &schedulerpb.ResponseHeader{ClusterId: s.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

func (s *heartbeatStreams) sendErr(errType schedulerpb.ErrorType, errMsg string, targetPeer *metapb.Peer) {
	msg := &schedulerpb.RegionHeartbeatResponse{
		Header: &schedulerpb.ResponseHeader{
			ClusterId: s.clusterID,
			Error: &schedulerpb.Error{
				Type:    errType,
				Message: errMsg,
			},
		},
		TargetPeer: targetPeer,
	}

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}
