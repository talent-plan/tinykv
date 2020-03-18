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

package tests

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TestServer states.
const (
	Initial int32 = iota
	Running
	Stop
	Destroy
)

// TestServer is only for test.
type TestServer struct {
	sync.RWMutex
	server *server.Server
	state  int32
}

var initHTTPClientOnce sync.Once

var zapLogOnce sync.Once

// NewTestServer creates a new TestServer.
func NewTestServer(cfg *config.Config) (*TestServer, error) {
	err := cfg.SetupLogger()
	if err != nil {
		return nil, err
	}
	zapLogOnce.Do(func() {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	})
	svr, err := server.CreateServer(cfg)
	if err != nil {
		return nil, err
	}
	initHTTPClientOnce.Do(func() {
		err = server.InitHTTPClient(svr)
	})
	if err != nil {
		return nil, err
	}
	return &TestServer{
		server: svr,
		state:  Initial,
	}, nil
}

// Run starts to run a TestServer.
func (s *TestServer) Run(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if s.state != Initial && s.state != Stop {
		return errors.Errorf("server(state%d) cannot run", s.state)
	}
	if err := s.server.Run(ctx); err != nil {
		return err
	}
	s.state = Running
	return nil
}

// Stop is used to stop a TestServer.
func (s *TestServer) Stop() error {
	s.Lock()
	defer s.Unlock()
	if s.state != Running {
		return errors.Errorf("server(state%d) cannot stop", s.state)
	}
	s.server.Close()
	s.state = Stop
	return nil
}

// Destroy is used to destroy a TestServer.
func (s *TestServer) Destroy() error {
	s.Lock()
	defer s.Unlock()
	dir := s.server.GetConfig().DataDir
	if s.state == Running {
		s.server.Close()
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	s.state = Destroy
	return nil
}

// ResignLeader resigns the leader of the server.
func (s *TestServer) ResignLeader() error {
	s.Lock()
	defer s.Unlock()
	return s.server.GetMember().ResignLeader(s.server.Context(), s.server.Name(), "")
}

// State returns the current TestServer's state.
func (s *TestServer) State() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.state
}

// GetConfig returns the current TestServer's configuration.
func (s *TestServer) GetConfig() *config.Config {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetConfig()
}

// GetAllocator returns the current TestServer's ID allocator.
func (s *TestServer) GetAllocator() *id.AllocatorImpl {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetAllocator()
}

// GetAddr returns the address of TestCluster.
func (s *TestServer) GetAddr() string {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetAddr()
}

// GetServer returns the real server of TestServer.
func (s *TestServer) GetServer() *server.Server {
	s.RLock()
	defer s.RUnlock()
	return s.server
}

// GetClusterID returns the cluster ID.
func (s *TestServer) GetClusterID() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.server.ClusterID()
}

// GetLeader returns current leader of PD cluster.
func (s *TestServer) GetLeader() *schedulerpb.Member {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetLeader()
}

// GetCluster returns PD cluster.
func (s *TestServer) GetCluster() *metapb.Cluster {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetCluster()
}

// GetServerID returns the unique etcd ID for this server in etcd cluster.
func (s *TestServer) GetServerID() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetMember().ID()
}

// IsLeader returns whether the server is leader or not.
func (s *TestServer) IsLeader() bool {
	s.RLock()
	defer s.RUnlock()
	return !s.server.IsClosed() && s.server.GetMember().IsLeader()
}

// GetEtcdLeader returns the builtin etcd leader.
func (s *TestServer) GetEtcdLeader() (string, error) {
	s.RLock()
	defer s.RUnlock()
	req := &schedulerpb.GetMembersRequest{Header: &schedulerpb.RequestHeader{ClusterId: s.server.ClusterID()}}
	members, err := s.server.GetMembers(context.TODO(), req)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return members.GetEtcdLeader().GetName(), nil
}

// GetEtcdLeaderID returns the builtin etcd leader ID.
func (s *TestServer) GetEtcdLeaderID() (uint64, error) {
	s.RLock()
	defer s.RUnlock()
	req := &schedulerpb.GetMembersRequest{Header: &schedulerpb.RequestHeader{ClusterId: s.server.ClusterID()}}
	members, err := s.server.GetMembers(context.TODO(), req)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return members.GetEtcdLeader().GetMemberId(), nil
}

// MoveEtcdLeader moves etcd leader from old to new.
func (s *TestServer) MoveEtcdLeader(old, new uint64) error {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetMember().MoveEtcdLeader(context.Background(), old, new)
}

// GetEtcdClient returns the builtin etcd client.
func (s *TestServer) GetEtcdClient() *clientv3.Client {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetClient()
}

// GetStores returns the stores of the cluster.
func (s *TestServer) GetStores() []*metapb.Store {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetMetaStores()
}

// GetStore returns the store with a given store ID.
func (s *TestServer) GetStore(storeID uint64) *core.StoreInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetStore(storeID)
}

// GetRaftCluster returns Raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *TestServer) GetRaftCluster() *server.RaftCluster {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster()
}

// GetRegions returns all regions' information in detail.
func (s *TestServer) GetRegions() []*core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetRegions()
}

// GetRegionInfoByID returns regionInfo by regionID from cluster.
func (s *TestServer) GetRegionInfoByID(regionID uint64) *core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetRegion(regionID)
}

// GetStoreRegions returns all regions' information with a given storeID.
func (s *TestServer) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetStoreRegions(storeID)
}

// CheckHealth checks if members are healthy.
func (s *TestServer) CheckHealth(members []*schedulerpb.Member) map[uint64]*schedulerpb.Member {
	s.RLock()
	defer s.RUnlock()
	return s.server.CheckHealth(members)
}

// BootstrapCluster is used to bootstrap the cluster.
func (s *TestServer) BootstrapCluster() error {
	bootstrapReq := &schedulerpb.BootstrapRequest{
		Header: &schedulerpb.RequestHeader{ClusterId: s.GetClusterID()},
		Store:  &metapb.Store{Id: 1, Address: "mock://1"},
	}
	_, err := s.server.Bootstrap(context.Background(), bootstrapReq)
	if err != nil {
		return err
	}
	return nil
}

// TestCluster is only for test.
type TestCluster struct {
	config  *clusterConfig
	servers map[string]*TestServer
}

// ConfigOption is used to define customize settings in test.
type ConfigOption func(conf *config.Config)

// NewTestCluster creates a new TestCluster.
func NewTestCluster(initialServerCount int, opts ...ConfigOption) (*TestCluster, error) {
	config := newClusterConfig(initialServerCount)
	servers := make(map[string]*TestServer)
	for _, conf := range config.InitialServers {
		serverConf, err := conf.Generate(opts...)
		if err != nil {
			return nil, err
		}
		s, err := NewTestServer(serverConf)
		if err != nil {
			return nil, err
		}
		servers[conf.Name] = s
	}
	return &TestCluster{
		config:  config,
		servers: servers,
	}, nil
}

// RunServer starts to run TestServer.
func (c *TestCluster) RunServer(ctx context.Context, server *TestServer) <-chan error {
	resC := make(chan error)
	go func() { resC <- server.Run(ctx) }()
	return resC
}

// RunServers starts to run multiple TestServer.
func (c *TestCluster) RunServers(ctx context.Context, servers []*TestServer) error {
	res := make([]<-chan error, len(servers))
	for i, s := range servers {
		res[i] = c.RunServer(ctx, s)
	}
	for _, c := range res {
		if err := <-c; err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// RunInitialServers starts to run servers in InitialServers.
func (c *TestCluster) RunInitialServers() error {
	var servers []*TestServer
	for _, conf := range c.config.InitialServers {
		servers = append(servers, c.GetServer(conf.Name))
	}
	return c.RunServers(context.Background(), servers)
}

// StopAll is used to stop all servers.
func (c *TestCluster) StopAll() error {
	for _, s := range c.servers {
		if err := s.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// GetServer returns a server with a given name.
func (c *TestCluster) GetServer(name string) *TestServer {
	return c.servers[name]
}

// GetServers returns all servers.
func (c *TestCluster) GetServers() map[string]*TestServer {
	return c.servers
}

// GetLeader returns the leader of all servers
func (c *TestCluster) GetLeader() string {
	for name, s := range c.servers {
		if s.IsLeader() {
			return name
		}
	}
	return ""
}

// WaitLeader is used to get leader.
// If it exceeds the maximum number of loops, it will return an empty string.
func (c *TestCluster) WaitLeader() string {
	for i := 0; i < 100; i++ {
		counter := make(map[string]int)
		running := 0
		for _, s := range c.servers {
			if s.state == Running {
				running++
			}
			n := s.GetLeader().GetName()
			if n != "" {
				counter[n]++
			}
		}
		for name, num := range counter {
			if num == running && c.GetServer(name).IsLeader() {
				return name
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ""
}

// ResignLeader resigns the leader of the cluster.
func (c *TestCluster) ResignLeader() error {
	leader := c.GetLeader()
	if len(leader) != 0 {
		return c.servers[leader].ResignLeader()
	}
	return errors.New("no leader")
}

// GetCluster returns PD cluster.
func (c *TestCluster) GetCluster() *metapb.Cluster {
	leader := c.GetLeader()
	return c.servers[leader].GetCluster()
}

// GetEtcdClient returns the builtin etcd client.
func (c *TestCluster) GetEtcdClient() *clientv3.Client {
	leader := c.GetLeader()
	return c.servers[leader].GetEtcdClient()
}

// GetConfig returns the current TestCluster's configuration.
func (c *TestCluster) GetConfig() *clusterConfig {
	return c.config
}

// CheckHealth checks if members are healthy.
func (c *TestCluster) CheckHealth(members []*schedulerpb.Member) map[uint64]*schedulerpb.Member {
	leader := c.GetLeader()
	return c.servers[leader].CheckHealth(members)
}

// HandleRegionHeartbeat processes RegionInfo reports from the client.
func (c *TestCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	leader := c.GetLeader()
	cluster := c.servers[leader].GetRaftCluster()
	return cluster.HandleRegionHeartbeat(region)
}

// Destroy is used to destroy a TestCluster.
func (c *TestCluster) Destroy() {
	for _, s := range c.servers {
		err := s.Destroy()
		if err != nil {
			log.Error("failed to destroy the cluster:", zap.Error(err))
		}
	}
}
