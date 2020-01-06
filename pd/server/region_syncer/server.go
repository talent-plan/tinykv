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

package syncer

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pingcap-incubator/tinykv/pd/server/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	msgSize                  = 8 * 1024 * 1024
	defaultBucketRate        = 20 * 1024 * 1024 // 20MB/s
	defaultBucketCapacity    = 20 * 1024 * 1024 // 20MB
	maxSyncRegionBatchSize   = 100
	syncerKeepAliveInterval  = 10 * time.Second
	defaultHistoryBufferSize = 10000
)

// ClientStream is the client side of the region syncer.
type ClientStream interface {
	Recv() (*pdpb.SyncRegionResponse, error)
	CloseSend() error
}

// ServerStream is the server side of the region syncer.
type ServerStream interface {
	Send(regions *pdpb.SyncRegionResponse) error
}

// Server is the abstraction of the syncer storage server.
type Server interface {
	Context() context.Context
	ClusterID() uint64
	GetMemberInfo() *pdpb.Member
	GetLeader() *pdpb.Member
	GetStorage() *core.Storage
	Name() string
	GetMetaRegions() []*metapb.Region
	GetSecurityConfig() *config.SecurityConfig
}

// RegionSyncer is used to sync the region information without raft.
type RegionSyncer struct {
	sync.RWMutex
	streams            map[string]ServerStream
	regionSyncerCtx    context.Context
	regionSyncerCancel context.CancelFunc
	server             Server
	closed             chan struct{}
	wg                 sync.WaitGroup
	history            *historyBuffer
	limit              *ratelimit.Bucket
	securityConfig     *config.SecurityConfig
}

// NewRegionSyncer returns a region syncer.
// The final consistency is ensured by the heartbeat.
// Strong consistency is not guaranteed.
// Usually open the region syncer in huge cluster and the server
// no longer etcd but go-leveldb.
func NewRegionSyncer(s Server) *RegionSyncer {
	return &RegionSyncer{
		streams:        make(map[string]ServerStream),
		server:         s,
		closed:         make(chan struct{}),
		history:        newHistoryBuffer(defaultHistoryBufferSize, s.GetStorage().GetRegionStorage()),
		limit:          ratelimit.NewBucketWithRate(defaultBucketRate, defaultBucketCapacity),
		securityConfig: s.GetSecurityConfig(),
	}
}

// RunServer runs the server of the region syncer.
// regionNitifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(regionNotifier <-chan *core.RegionInfo, quit chan struct{}) {
	var requests []*metapb.Region
	ticker := time.NewTicker(syncerKeepAliveInterval)
	for {
		select {
		case <-quit:
			log.Info("region syncer has been stopped")
			return
		case first := <-regionNotifier:
			requests = append(requests, first.GetMeta())
			startIndex := s.history.GetNextIndex()
			s.history.Record(first)
			pending := len(regionNotifier)
			for i := 0; i < pending && i < maxSyncRegionBatchSize; i++ {
				region := <-regionNotifier
				requests = append(requests, region.GetMeta())
				s.history.Record(region)
			}
			regions := &pdpb.SyncRegionResponse{
				Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				Regions:    requests,
				StartIndex: startIndex,
			}
			s.broadcast(regions)
		case <-ticker.C:
			alive := &pdpb.SyncRegionResponse{
				Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				StartIndex: s.history.GetNextIndex(),
			}
			s.broadcast(alive)
		}
		requests = requests[:0]
	}
}

// Sync firstly tries to sync the history records to client.
// then to sync the latest records.
func (s *RegionSyncer) Sync(stream pdpb.PD_SyncRegionsServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		clusterID := request.GetHeader().GetClusterId()
		if clusterID != s.server.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.server.ClusterID(), clusterID)
		}
		log.Info("establish sync region stream",
			zap.String("requested-server", request.GetMember().GetName()),
			zap.String("url", request.GetMember().GetClientUrls()[0]))

		err = s.syncHistoryRegion(request, stream)
		if err != nil {
			return err
		}
		s.bindStream(request.GetMember().GetName(), stream)
	}
}

func (s *RegionSyncer) syncHistoryRegion(request *pdpb.SyncRegionRequest, stream pdpb.PD_SyncRegionsServer) error {
	startIndex := request.GetStartIndex()
	name := request.GetMember().GetName()
	records := s.history.RecordsFrom(startIndex)
	if len(records) == 0 {
		if s.history.GetNextIndex() == startIndex {
			log.Info("requested server has already in sync with server",
				zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Uint64("last-index", startIndex))
			return nil
		}
		// do full synchronization
		if startIndex == 0 {
			regions := s.server.GetMetaRegions()
			lastIndex := 0
			start := time.Now()
			res := make([]*metapb.Region, 0, maxSyncRegionBatchSize)
			for syncedIndex, r := range regions {
				res = append(res, r)
				if len(res) < maxSyncRegionBatchSize && syncedIndex < len(regions)-1 {
					continue
				}
				resp := &pdpb.SyncRegionResponse{
					Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
					Regions:    res,
					StartIndex: uint64(lastIndex),
				}
				s.limit.Wait(int64(resp.Size()))
				lastIndex += len(res)
				if err := stream.Send(resp); err != nil {
					log.Error("failed to send sync region response", zap.Error(err))
				}
				res = res[:0]
			}
			log.Info("requested server has completed full synchronization with server",
				zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Duration("cost", time.Since(start)))
			return nil
		}
		log.Warn("no history regions from index, the leader may be restarted", zap.Uint64("index", startIndex))
		return nil
	}
	log.Info("sync the history regions with server",
		zap.String("server", name),
		zap.Uint64("from-index", startIndex),
		zap.Uint64("last-index", s.history.GetNextIndex()),
		zap.Int("records-length", len(records)))
	regions := make([]*metapb.Region, len(records))
	for i, r := range records {
		regions[i] = r.GetMeta()
	}
	resp := &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
		Regions:    regions,
		StartIndex: startIndex,
	}
	return stream.Send(resp)
}

// bindStream binds the established server stream.
func (s *RegionSyncer) bindStream(name string, stream ServerStream) {
	s.Lock()
	defer s.Unlock()
	s.streams[name] = stream
}

func (s *RegionSyncer) broadcast(regions *pdpb.SyncRegionResponse) {
	var failed []string
	s.RLock()
	for name, sender := range s.streams {
		err := sender.Send(regions)
		if err != nil {
			log.Error("region syncer send data meet error", zap.Error(err))
			failed = append(failed, name)
		}
	}
	s.RUnlock()
	if len(failed) > 0 {
		s.Lock()
		for _, name := range failed {
			delete(s.streams, name)
			log.Info("region syncer delete the stream", zap.String("stream", name))
		}
		s.Unlock()
	}
}
