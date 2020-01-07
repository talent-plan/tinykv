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
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap-incubator/tinykv/scheduler/server/kv"
	"github.com/pingcap-incubator/tinykv/scheduler/server/member"
	"github.com/pingcap-incubator/tinykv/scheduler/server/tso"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	// ErrNotBootstrapped is error info for cluster not bootstrapped.
	ErrNotBootstrapped = errors.New("TiKV cluster not bootstrapped, please start TiKV first")
	// ErrServerNotStarted is error info for server not started.
	ErrServerNotStarted = errors.New("The server has not been started")
	// ErrOperatorNotFound is error info for operator not found.
	ErrOperatorNotFound = errors.New("operator not found")
	// ErrAddOperator is error info for already have an operator when adding operator.
	ErrAddOperator = errors.New("failed to add operator, maybe already have one")
	// ErrRegionNotAdjacent is error info for region not adjacent.
	ErrRegionNotAdjacent = errors.New("two regions are not adjacent")
	// ErrRegionNotFound is error info for region not found.
	ErrRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	// ErrRegionAbnormalPeer is error info for region has abonormal peer.
	ErrRegionAbnormalPeer = func(regionID uint64) error {
		return errors.Errorf("region %v has abnormal peer", regionID)
	}
	// ErrRegionIsStale is error info for region is stale.
	ErrRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
	// ErrStoreNotFound is error info for store not found.
	ErrStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
)

const (
	etcdTimeout           = time.Second * 3
	etcdStartTimeout      = time.Minute * 5
	serverMetricsInterval = time.Minute
	leaderTickInterval    = 50 * time.Millisecond
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

// EnableZap enable the zap logger in embed etcd.
var EnableZap = false

// Server is the pd server.
type Server struct {
	// Server state.
	isServing int64

	// Configs and initial fields.
	cfg         *config.Config
	etcdCfg     *embed.Config
	scheduleOpt *config.ScheduleOption

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	member    *member.Member
	client    *clientv3.Client
	clusterID uint64 // pd cluster id.
	rootPath  string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAllocator *id.AllocatorImpl
	// for storage operation.
	storage *core.Storage
	// for tso.
	tso *tso.TimestampOracle
	// for raft cluster
	cluster *RaftCluster
	// For async region heartbeat.
	hbStreams *heartbeatStreams
	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(cfg *config.Config) (*Server, error) {
	log.Info("PD Config", zap.Reflect("config", cfg))
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:         cfg,
		scheduleOpt: config.NewScheduleOption(cfg),
		member:      &member.Member{},
	}

	// Adjust etcd config.
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) { pdpb.RegisterPDServer(gs, s) }
	s.etcdCfg = etcdCfg
	if EnableZap {
		// The etcd master version has removed embed.Config.SetupLogging.
		// Now logger is set up automatically based on embed.Config.Logger,
		// Use zap logger in the test, otherwise will panic.
		// Reference: https://go.etcd.io/etcd/blob/master/embed/config_logging.go#L45
		s.etcdCfg.Logger = "zap"
		s.etcdCfg.LogOutputs = []string{"stdout"}
	}
	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	log.Info("start embed etcd")
	ctx, cancel := context.WithTimeout(ctx, etcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errors.WithStack(err)
	}

	// Check cluster ID
	urlmap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errors.WithStack(err)
	}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap, tlsConfig); err != nil {
		return err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-ctx.Done():
		return errors.Errorf("canceled when waiting embed etcd to be ready")
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Info("update advertise peer urls", zap.String("from", s.cfg.AdvertisePeerUrls), zap.String("to", etcdPeerURLs))
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
	s.client = client
	s.member = member.NewMember(etcd, client, etcdServerID)
	return nil
}

func (s *Server) startServer(ctx context.Context) error {
	var err error
	if err = s.initClusterID(); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.member.MemberInfo(s.cfg, s.Name(), s.rootPath)

	s.idAllocator = id.NewAllocatorImpl(s.client, s.rootPath, s.member.MemberValue())
	s.tso = tso.NewTimestampOracle(
		s.client,
		s.rootPath,
		s.member.MemberValue(),
		s.cfg.TsoSaveInterval.Duration,
		func() time.Duration { return s.scheduleOpt.LoadPDServerConfig().MaxResetTSGap },
	)
	kvBase := kv.NewEtcdKVBase(s.client, s.rootPath)
	path := filepath.Join(s.cfg.DataDir, "region-meta")
	regionStorage, err := core.NewRegionStorage(ctx, path)
	if err != nil {
		return err
	}
	s.storage = core.NewStorage(kvBase).SetRegionStorage(regionStorage)
	s.cluster = newRaftCluster(ctx, s, s.clusterID)
	s.hbStreams = newHeartbeatStreams(ctx, s.clusterID, s.cluster)
	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := etcdutil.EtcdKVGet(s.client, pdClusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return err
	}
	s.clusterID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
	return err
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.stopServerLoop()

	if s.client != nil {
		s.client.Close()
	}

	if s.member.Etcd() != nil {
		s.member.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}
	if err := s.storage.Close(); err != nil {
		log.Error("close storage meet error", zap.Error(err))
	}

	log.Info("close server")
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// Run runs the pd server.
func (s *Server) Run(ctx context.Context) error {
	go StartMonitor(ctx, time.Now, func() {
		log.Error("system time jumps backward")
		timeJumpBackCounter.Inc()
	})

	if err := s.startEtcd(ctx); err != nil {
		return err
	}

	if err := s.startServer(ctx); err != nil {
		return err
	}

	s.startServerLoop(ctx)

	return nil
}

// Context returns the loop context of server.
func (s *Server) Context() context.Context {
	return s.serverLoopCtx
}

func (s *Server) startServerLoop(ctx context.Context) {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	s.serverLoopWg.Add(3)
	go s.leaderLoop()
	go s.etcdLeaderLoop()
	go s.serverMetricsLoop()
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) serverMetricsLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(serverMetricsInterval):
			s.collectEtcdStateMetrics()
		case <-ctx.Done():
			log.Info("server is closed, exit metrics loop")
			return
		}
	}
}

func (s *Server) collectEtcdStateMetrics() {
	etcdStateGauge.WithLabelValues("term").Set(float64(s.member.Etcd().Server.Term()))
	etcdStateGauge.WithLabelValues("appliedIndex").Set(float64(s.member.Etcd().Server.AppliedIndex()))
	etcdStateGauge.WithLabelValues("committedIndex").Set(float64(s.member.Etcd().Server.CommittedIndex()))
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	clusterID := s.clusterID

	log.Info("try to bootstrap raft cluster",
		zap.Uint64("cluster-id", clusterID),
		zap.String("request", fmt.Sprintf("%v", req)))

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, err
	}

	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.GetReplication().GetMaxReplicas()),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	clusterRootPath := s.getClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	bootstrapKey := makeBootstrapTimeKey(clusterRootPath)
	nano := time.Now().UnixNano()

	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(bootstrapKey, string(timeData)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := req.GetRegion().Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := kv.NewSlowLogTxn(s.client).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !resp.Succeeded {
		log.Warn("cluster already bootstrapped", zap.Uint64("cluster-id", clusterID))
		return nil, errors.Errorf("cluster %d already bootstrapped", clusterID)
	}

	log.Info("bootstrap cluster ok", zap.Uint64("cluster-id", clusterID))
	err = s.storage.SaveRegion(req.GetRegion())
	if err != nil {
		log.Warn("save the bootstrap region failed", zap.Error(err))
	}
	err = s.storage.Flush()
	if err != nil {
		log.Warn("flush the bootstrap region failed", zap.Error(err))
	}
	if err := s.cluster.start(); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{}, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	return s.cluster.start()
}

func (s *Server) stopRaftCluster() {
	s.cluster.stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetMemberInfo returns the server member information.
func (s *Server) GetMemberInfo() *pdpb.Member {
	return proto.Clone(s.member.Member()).(*pdpb.Member)
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetLeader returns leader of etcd.
func (s *Server) GetLeader() *pdpb.Member {
	return s.member.GetLeader()
}

// GetMember returns the member of server.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// GetStorage returns the backend storage of server.
func (s *Server) GetStorage() *core.Storage {
	return s.storage
}

// GetAllocator returns the ID allocator of server.
func (s *Server) GetAllocator() *id.AllocatorImpl {
	return s.idAllocator
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.Schedule = *s.scheduleOpt.Load()
	cfg.Replication = *s.scheduleOpt.GetReplication().Load()
	cfg.LabelProperty = s.scheduleOpt.LoadLabelPropertyConfig().Clone()
	cfg.ClusterVersion = *s.scheduleOpt.LoadClusterVersion()
	cfg.PDServerCfg = *s.scheduleOpt.LoadPDServerConfig()
	storage := s.GetStorage()
	if storage == nil {
		return cfg
	}
	sches, configs, err := storage.LoadAllScheduleConfig()
	if err != nil {
		return cfg
	}
	payload := make(map[string]string)
	for i, sche := range sches {
		payload[sche] = configs[i]
	}
	cfg.Schedule.SchedulersPayload = payload
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *config.ScheduleConfig {
	cfg := &config.ScheduleConfig{}
	*cfg = *s.scheduleOpt.Load()
	return cfg
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg config.ScheduleConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.Deprecated(); err != nil {
		return err
	}
	old := s.scheduleOpt.Load()
	s.scheduleOpt.Store(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.Store(old)
		log.Error("failed to update schedule config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("schedule config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *config.ReplicationConfig {
	cfg := &config.ReplicationConfig{}
	*cfg = *s.scheduleOpt.GetReplication().Load()
	return cfg
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg config.ReplicationConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	old := s.scheduleOpt.GetReplication().Load()
	s.scheduleOpt.GetReplication().Store(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.GetReplication().Store(old)
		log.Error("failed to update replication config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("replication config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetPDServerConfig sets the server config.
func (s *Server) SetPDServerConfig(cfg config.PDServerConfig) error {
	old := s.scheduleOpt.LoadPDServerConfig()
	s.scheduleOpt.SetPDServerConfig(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.SetPDServerConfig(old)
		log.Error("failed to update PDServer config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("PD server config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelProperty inserts a label property config.
func (s *Server) SetLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.SetLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.DeleteLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to update label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()),
			zap.Error(err))
		return err
	}
	log.Info("label property config is updated", zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()))
	return nil
}

// DeleteLabelProperty deletes a label property config.
func (s *Server) DeleteLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.DeleteLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.SetLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to delete label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()),
			zap.Error(err))
		return err
	}
	log.Info("label property config is deleted", zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()))
	return nil
}

// GetLabelProperty returns the whole label property config.
func (s *Server) GetLabelProperty() config.LabelPropertyConfig {
	return s.scheduleOpt.LoadLabelPropertyConfig().Clone()
}

// SetClusterVersion sets the version of cluster.
func (s *Server) SetClusterVersion(v string) error {
	version, err := ParseVersion(v)
	if err != nil {
		return err
	}
	old := s.scheduleOpt.LoadClusterVersion()
	s.scheduleOpt.SetClusterVersion(version)
	err = s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.SetClusterVersion(old)
		log.Error("failed to update cluster version",
			zap.String("old-version", old.String()),
			zap.String("new-version", v),
			zap.Error(err))
		return err
	}
	log.Info("cluster version is updated", zap.String("new-version", v))
	return nil
}

// GetClusterVersion returns the version of cluster.
func (s *Server) GetClusterVersion() semver.Version {
	return *s.scheduleOpt.LoadClusterVersion()
}

// GetSecurityConfig get the security config.
func (s *Server) GetSecurityConfig() *config.SecurityConfig {
	return &s.cfg.Security
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets Raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *RaftCluster {
	if s.IsClosed() || !s.cluster.isRunning() {
		return nil
	}
	return s.cluster
}

// GetCluster gets cluster.
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.GetReplication().GetMaxReplicas()),
	}
}

// GetMetaRegions gets meta regions from cluster.
func (s *Server) GetMetaRegions() []*metapb.Region {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		return cluster.GetMetaRegions()
	}
	return nil
}

// GetClusterStatus gets cluster status.
func (s *Server) GetClusterStatus() (*ClusterStatus, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	return s.cluster.loadClusterStatus()
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) {
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
}

var healthURL = "/pd/ping"

// CheckHealth checks if members are healthy.
func (s *Server) CheckHealth(members []*pdpb.Member) map[uint64]*pdpb.Member {
	unhealthMembers := make(map[uint64]*pdpb.Member)
	for _, member := range members {
		for _, cURL := range member.ClientUrls {
			resp, err := dialClient.Get(fmt.Sprintf("%s%s", cURL, healthURL))
			if resp != nil {
				resp.Body.Close()
			}
			if err != nil || resp.StatusCode != http.StatusOK {
				unhealthMembers[member.GetMemberId()] = member
				break
			}
		}
	}
	return unhealthMembers
}

func (s *Server) leaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.IsClosed() {
			log.Info("server is closed, return leader loop")
			return
		}

		leader, rev, checkAgain := s.member.CheckLeader(s.Name())
		if checkAgain {
			continue
		}
		if leader != nil {
			err := s.reloadConfigFromKV()
			if err != nil {
				log.Error("reload config failed", zap.Error(err))
				continue
			}
			if s.scheduleOpt.LoadPDServerConfig().UseRegionStorage {
				s.cluster.regionSyncer.StartSyncWithLeader(leader.GetClientUrls()[0])
			}
			log.Info("start watch leader", zap.Stringer("leader", leader))
			s.member.WatchLeader(s.serverLoopCtx, leader, rev)
			s.cluster.regionSyncer.StopSyncWithLeader()
			log.Info("leader changed, try to campaign leader")
		}

		etcdLeader := s.member.GetEtcdLeader()
		if etcdLeader != s.member.ID() {
			log.Info("skip campaign leader and check later",
				zap.String("server-name", s.Name()),
				zap.Uint64("etcd-leader-id", etcdLeader))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign leader", zap.String("campaign-leader-name", s.Name()))

	lease := member.NewLeaderLease(s.client)
	defer lease.Close()
	if err := s.member.CampaignLeader(lease, s.cfg.LeaderLease); err != nil {
		log.Error("campaign leader meet error", zap.Error(err))
		return
	}

	// Start keepalive and enable TSO service.
	// TSO service is strictly enabled/disabled by leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	go lease.KeepAlive(ctx)
	log.Info("campaign leader ok", zap.String("campaign-leader-name", s.Name()))

	log.Debug("sync timestamp for tso")
	if err := s.tso.SyncTimestamp(lease); err != nil {
		log.Error("failed to sync timestamp", zap.Error(err))
		return
	}
	defer s.tso.ResetTimestamp()

	err := s.reloadConfigFromKV()
	if err != nil {
		log.Error("failed to reload configuration", zap.Error(err))
		return
	}
	// Try to create raft cluster.
	err = s.createRaftCluster()
	if err != nil {
		log.Error("failed to create raft cluster", zap.Error(err))
		return
	}
	defer s.stopRaftCluster()

	s.member.EnableLeader()
	defer s.member.DisableLeader()

	CheckPDVersion(s.scheduleOpt)
	log.Info("PD cluster leader is ready to serve", zap.String("leader-name", s.Name()))

	tsTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsTicker.Stop()
	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if lease.IsExpired() {
				log.Info("lease expired, leader step down")
				return
			}
			etcdLeader := s.member.GetEtcdLeader()
			if etcdLeader != s.member.ID() {
				log.Info("etcd leader changed, resigns leadership", zap.String("old-leader-name", s.Name()))
				return
			}
		case <-tsTicker.C:
			if err = s.tso.UpdateTimestamp(); err != nil {
				log.Error("failed to update timestamp", zap.Error(err))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return
		}
	}
}

func (s *Server) etcdLeaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(s.cfg.LeaderPriorityCheckInterval.Duration):
			s.member.CheckPriority(ctx)
		case <-ctx.Done():
			log.Info("server is closed, exit etcd leader loop")
			return
		}
	}
}

func (s *Server) reloadConfigFromKV() error {
	err := s.scheduleOpt.Reload(s.storage)
	if err != nil {
		return err
	}
	if s.scheduleOpt.LoadPDServerConfig().UseRegionStorage {
		s.storage.SwitchToRegionStorage()
		log.Info("server enable region storage")
	} else {
		s.storage.SwitchToDefaultStorage()
		log.Info("server disable region storage")
	}
	return nil
}
