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
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const slowThreshold = 5 * time.Millisecond

// notLeaderError is returned when current server is not the leader and not possible to process request.
// TODO: work as proxy.
var notLeaderError = status.Errorf(codes.Unavailable, "not leader")

// GetMembers implements gRPC PDServer.
func (s *Server) GetMembers(context.Context, *schedulerpb.GetMembersRequest) (*schedulerpb.GetMembersResponse, error) {
	if s.IsClosed() {
		return nil, status.Errorf(codes.Unknown, "server not started")
	}
	members, err := GetMembers(s.GetClient())
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	var etcdLeader *schedulerpb.Member
	leadID := s.member.GetEtcdLeader()
	for _, m := range members {
		if m.MemberId == leadID {
			etcdLeader = m
			break
		}
	}

	return &schedulerpb.GetMembersResponse{
		Header:     s.header(),
		Members:    members,
		Leader:     s.member.GetLeader(),
		EtcdLeader: etcdLeader,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *Server) Tso(stream schedulerpb.Scheduler_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.clusterID {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.tso.GetRespTS(count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}

		elapsed := time.Since(start)
		if elapsed > slowThreshold {
			log.Warn("get timestamp too slow", zap.Duration("cost", elapsed))
		}
		response := &schedulerpb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// Bootstrap implements gRPC PDServer.
func (s *Server) Bootstrap(ctx context.Context, request *schedulerpb.BootstrapRequest) (*schedulerpb.BootstrapResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster != nil {
		err := &schedulerpb.Error{
			Type:    schedulerpb.ErrorType_ALREADY_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
		return &schedulerpb.BootstrapResponse{
			Header: s.errorHeader(err),
		}, nil
	}
	if _, err := s.bootstrapCluster(request); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &schedulerpb.BootstrapResponse{
		Header: s.header(),
	}, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *Server) IsBootstrapped(ctx context.Context, request *schedulerpb.IsBootstrappedRequest) (*schedulerpb.IsBootstrappedResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	return &schedulerpb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: cluster != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *Server) AllocID(ctx context.Context, request *schedulerpb.AllocIDRequest) (*schedulerpb.AllocIDResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAllocator.Alloc()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &schedulerpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *Server) GetStore(ctx context.Context, request *schedulerpb.GetStoreRequest) (*schedulerpb.GetStoreResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	store := cluster.GetStore(storeID)
	if store == nil {
		return nil, status.Errorf(codes.Unknown, "invalid store ID %d, not found", storeID)
	}
	return &schedulerpb.GetStoreResponse{
		Header: s.header(),
		Store:  store.GetMeta(),
		Stats:  store.GetStoreStats(),
	}, nil
}

// checkStore2 returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
// Copied from server/command.go
func checkStore2(cluster *RaftCluster, storeID uint64) *schedulerpb.Error {
	store := cluster.GetStore(storeID)
	if store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return &schedulerpb.Error{
				Type:    schedulerpb.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// PutStore implements gRPC PDServer.
func (s *Server) PutStore(ctx context.Context, request *schedulerpb.PutStoreRequest) (*schedulerpb.PutStoreResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.PutStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore2(cluster, store.GetId()); pberr != nil {
		return &schedulerpb.PutStoreResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	if err := cluster.putStore(store); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put store ok", zap.Stringer("store", store))

	return &schedulerpb.PutStoreResponse{
		Header: s.header(),
	}, nil
}

// GetAllStores implements gRPC PDServer.
func (s *Server) GetAllStores(ctx context.Context, request *schedulerpb.GetAllStoresRequest) (*schedulerpb.GetAllStoresResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetAllStoresResponse{Header: s.notBootstrappedHeader()}, nil
	}

	// Don't return tombstone stores.
	var stores []*metapb.Store
	if request.GetExcludeTombstoneStores() {
		for _, store := range cluster.GetMetaStores() {
			if store.GetState() != metapb.StoreState_Tombstone {
				stores = append(stores, store)
			}
		}
	} else {
		stores = cluster.GetMetaStores()
	}

	return &schedulerpb.GetAllStoresResponse{
		Header: s.header(),
		Stores: stores,
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Server) StoreHeartbeat(ctx context.Context, request *schedulerpb.StoreHeartbeatRequest) (*schedulerpb.StoreHeartbeatResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if pberr := checkStore2(cluster, request.GetStats().GetStoreId()); pberr != nil {
		return &schedulerpb.StoreHeartbeatResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	err := cluster.handleStoreHeartbeat(request.Stats)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &schedulerpb.StoreHeartbeatResponse{
		Header: s.header(),
	}, nil
}

const regionHeartbeatSendTimeout = 5 * time.Second

var errSendRegionHeartbeatTimeout = errors.New("send region heartbeat timeout")

// heartbeatServer wraps Scheduler_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream schedulerpb.Scheduler_RegionHeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m *schedulerpb.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() { done <- s.stream.Send(m) }()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-time.After(regionHeartbeatSendTimeout):
		atomic.StoreInt32(&s.closed, 1)
		return errors.WithStack(errSendRegionHeartbeatTimeout)
	}
}

func (s *heartbeatServer) Recv() (*schedulerpb.RegionHeartbeatRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	req, err := s.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&s.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(stream schedulerpb.Scheduler_RegionHeartbeatServer) error {
	server := &heartbeatServer{stream: stream}
	cluster := s.GetRaftCluster()
	if cluster == nil {
		resp := &schedulerpb.RegionHeartbeatResponse{
			Header: s.notBootstrappedHeader(),
		}
		err := server.Send(resp)
		return errors.WithStack(err)
	}

	var lastBind time.Time
	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}

		storeID := request.GetLeader().GetStoreId()
		store := cluster.GetStore(storeID)
		if store == nil {
			return errors.Errorf("invalid store ID %d, not found", storeID)
		}

		hbStreams := cluster.GetHeartbeatStreams()

		if time.Since(lastBind) > s.cfg.HeartbeatStreamBindInterval.Duration {
			hbStreams.bindStream(storeID, server)
			lastBind = time.Now()
		}

		region := core.RegionFromHeartbeat(request)
		if region.GetLeader() == nil {
			log.Error("invalid request, the leader is nil", zap.Reflect("reqeust", request))
			continue
		}
		if region.GetID() == 0 {
			msg := fmt.Sprintf("invalid request region, %v", request)
			hbStreams.sendErr(schedulerpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}

		err = cluster.HandleRegionHeartbeat(region)
		if err != nil {
			msg := err.Error()
			hbStreams.sendErr(schedulerpb.ErrorType_UNKNOWN, msg, request.GetLeader())
		}
	}
}

// GetRegion implements gRPC PDServer.
func (s *Server) GetRegion(ctx context.Context, request *schedulerpb.GetRegionRequest) (*schedulerpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region, leader := cluster.GetRegionByKey(request.GetRegionKey())
	return &schedulerpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func (s *Server) GetPrevRegion(ctx context.Context, request *schedulerpb.GetRegionRequest) (*schedulerpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	region, leader := cluster.GetPrevRegionByKey(request.GetRegionKey())
	return &schedulerpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *Server) GetRegionByID(ctx context.Context, request *schedulerpb.GetRegionByIDRequest) (*schedulerpb.GetRegionResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	id := request.GetRegionId()
	region, leader := cluster.GetRegionByID(id)
	return &schedulerpb.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// ScanRegions implements gRPC PDServer.
func (s *Server) ScanRegions(ctx context.Context, request *schedulerpb.ScanRegionsRequest) (*schedulerpb.ScanRegionsResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.ScanRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	regions := cluster.ScanRegions(request.GetStartKey(), request.GetEndKey(), int(request.GetLimit()))
	resp := &schedulerpb.ScanRegionsResponse{Header: s.header()}
	for _, r := range regions {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		resp.Regions = append(resp.Regions, r.GetMeta())
		resp.Leaders = append(resp.Leaders, leader)
	}
	return resp, nil
}

// AskSplit implements gRPC PDServer.
func (s *Server) AskSplit(ctx context.Context, request *schedulerpb.AskSplitRequest) (*schedulerpb.AskSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.AskSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	req := &schedulerpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := cluster.handleAskSplit(req)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &schedulerpb.AskSplitResponse{
		Header:      s.header(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *Server) ReportSplit(ctx context.Context, request *schedulerpb.ReportSplitRequest) (*schedulerpb.ReportSplitResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.ReportSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	_, err := cluster.handleReportSplit(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &schedulerpb.ReportSplitResponse{
		Header: s.header(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *Server) GetClusterConfig(ctx context.Context, request *schedulerpb.GetClusterConfigRequest) (*schedulerpb.GetClusterConfigResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	return &schedulerpb.GetClusterConfigResponse{
		Header:  s.header(),
		Cluster: cluster.GetConfig(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *Server) PutClusterConfig(ctx context.Context, request *schedulerpb.PutClusterConfigRequest) (*schedulerpb.PutClusterConfigResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.PutClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := cluster.putConfig(conf); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put cluster config ok", zap.Reflect("config", conf))

	return &schedulerpb.PutClusterConfigResponse{
		Header: s.header(),
	}, nil
}

// ScatterRegion implements gRPC PDServer.
func (s *Server) ScatterRegion(ctx context.Context, request *schedulerpb.ScatterRegionRequest) (*schedulerpb.ScatterRegionResponse, error) {
	// if err := s.validateRequest(request.GetHeader()); err != nil {
	// 	return nil, err
	// }

	// cluster := s.GetRaftCluster()
	// if cluster == nil {
	// 	return &schedulerpb.ScatterRegionResponse{Header: s.notBootstrappedHeader()}, nil
	// }

	// region := cluster.GetRegion(request.GetRegionId())
	// if region == nil {
	// 	if request.GetRegion() == nil {
	// 		return nil, errors.Errorf("region %d not found", request.GetRegionId())
	// 	}
	// 	region = core.NewRegionInfo(request.GetRegion(), request.GetLeader())
	// }

	// if cluster.IsRegionHot(region) {
	// 	return nil, errors.Errorf("region %d is a hot region", region.GetID())
	// }

	// co := cluster.GetCoordinator()
	// op, err := co.regionScatterer.Scatter(region)
	// if err != nil {
	// 	return nil, err
	// }
	// if op != nil {
	// 	co.opController.AddOperator(op)
	// }

	return &schedulerpb.ScatterRegionResponse{
		Header: s.header(),
	}, nil
}

// GetGCSafePoint implements gRPC PDServer.
func (s *Server) GetGCSafePoint(ctx context.Context, request *schedulerpb.GetGCSafePointRequest) (*schedulerpb.GetGCSafePointResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	safePoint, err := s.storage.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &schedulerpb.GetGCSafePointResponse{
		Header:    s.header(),
		SafePoint: safePoint,
	}, nil
}

// UpdateGCSafePoint implements gRPC PDServer.
func (s *Server) UpdateGCSafePoint(ctx context.Context, request *schedulerpb.UpdateGCSafePointRequest) (*schedulerpb.UpdateGCSafePointResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.UpdateGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	oldSafePoint, err := s.storage.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	newSafePoint := request.SafePoint

	// Only save the safe point if it's greater than the previous one
	if newSafePoint > oldSafePoint {
		if err := s.storage.SaveGCSafePoint(newSafePoint); err != nil {
			return nil, err
		}
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint))
		newSafePoint = oldSafePoint
	}

	return &schedulerpb.UpdateGCSafePointResponse{
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

// GetOperator gets information about the operator belonging to the speicfy region.
func (s *Server) GetOperator(ctx context.Context, request *schedulerpb.GetOperatorRequest) (*schedulerpb.GetOperatorResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &schedulerpb.GetOperatorResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opController := cluster.coordinator.opController
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := s.errorHeader(&schedulerpb.Error{
			Type:    schedulerpb.ErrorType_REGION_NOT_FOUND,
			Message: "Not Found",
		})
		return &schedulerpb.GetOperatorResponse{Header: header}, nil
	}

	return &schedulerpb.GetOperatorResponse{
		Header:   s.header(),
		RegionId: requestID,
		Desc:     []byte(r.Op.Desc()),
		Kind:     []byte(r.Op.Kind().String()),
		Status:   r.Status,
	}, nil
}

// validateRequest checks if Server is leader and clusterID is matched.
// TODO: Call it in gRPC intercepter.
func (s *Server) validateRequest(header *schedulerpb.RequestHeader) error {
	if s.IsClosed() || !s.member.IsLeader() {
		return errors.WithStack(notLeaderError)
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *Server) header() *schedulerpb.ResponseHeader {
	return &schedulerpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Server) errorHeader(err *schedulerpb.Error) *schedulerpb.ResponseHeader {
	return &schedulerpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Server) notBootstrappedHeader() *schedulerpb.ResponseHeader {
	return s.errorHeader(&schedulerpb.Error{
		Type:    schedulerpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}
