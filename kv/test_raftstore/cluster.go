package test_raftstore

import (
	"context"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type Simulator interface {
	RunNode(raftConf *tikvConf.Config, engine *engine_util.Engines, ctx context.Context) error
	StopNode(nodeID uint64)
	AddFilter(filter Filter)
	ClearFilters()
	GetNodeIds() []uint64
	CallCommandOnNode(nodeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse
}

type Cluster struct {
	pdClient  pd.Client
	count     int
	engines   map[uint64]*engine_util.Engines
	snapPaths map[uint64]string
	dirs      []string
	simulator Simulator
}

func NewCluster(count int, pdClient pd.Client, simulator Simulator) *Cluster {
	return &Cluster{
		count:     count,
		pdClient:  pdClient,
		engines:   make(map[uint64]*engine_util.Engines),
		snapPaths: make(map[uint64]string),
		simulator: simulator,
	}
}

func (c *Cluster) Start() error {
	ctx := context.TODO()
	clusterID := c.pdClient.GetClusterID(ctx)

	for nodeID := uint64(1); nodeID <= uint64(c.count); nodeID++ {
		dbPath, err := ioutil.TempDir("", "test-raftstore")
		kvPath := filepath.Join(dbPath, "kv")
		raftPath := filepath.Join(dbPath, "raft")
		snapPath := filepath.Join(dbPath, "snap")
		c.snapPaths[nodeID] = snapPath
		c.dirs = append(c.dirs, []string{kvPath, raftPath, snapPath}...)

		err = os.MkdirAll(kvPath, os.ModePerm)
		if err != nil {
			panic(err)
		}
		err = os.MkdirAll(raftPath, os.ModePerm)
		if err != nil {
			panic(err)
		}
		err = os.MkdirAll(snapPath, os.ModePerm)
		if err != nil {
			panic(err)
		}

		conf := config.DefaultConf
		conf.Engine.DBPath = dbPath

		raftDB := engine_util.CreateDB("raft", &conf.Engine)
		kvDB := engine_util.CreateDB("kv", &conf.Engine)
		engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
		c.engines[nodeID] = engine
	}

	regionEpoch := &metapb.RegionEpoch{
		Version: raftstore.InitEpochVer,
		ConfVer: raftstore.InitEpochConfVer,
	}
	firstRegion := &metapb.Region{
		Id:          1,
		StartKey:    []byte{},
		EndKey:      []byte{},
		RegionEpoch: regionEpoch,
	}

	for nodeID, engine := range c.engines {
		peer := NewPeer(nodeID, nodeID)
		firstRegion.Peers = append(firstRegion.Peers, &peer)
		err := raftstore.BootstrapStore(engine, clusterID, nodeID)
		if err != nil {
			return err
		}
	}

	for _, engine := range c.engines {
		raftstore.PrepareBootstrapCluster(engine, firstRegion)
	}

	store := &metapb.Store{
		Id:      1,
		Address: "",
	}
	resp, err := c.pdClient.Bootstrap(context.TODO(), store, firstRegion)
	if err != nil {
		panic(err)
	}
	if resp.Header != nil && resp.Header.Error != nil {
		panic(resp.Header.Error)
	}

	for nodeID, engine := range c.engines {
		store := &metapb.Store{
			Id:      nodeID,
			Address: "",
		}
		err := c.pdClient.PutStore(context.TODO(), store)
		if err != nil {
			panic(err)
		}
		raftstore.ClearPrepareBootstrapState(engine)
	}

	for nodeID, engine := range c.engines {
		raftConf := tikvConf.NewDefaultConfig()
		raftConf.SnapPath = c.snapPaths[nodeID]
		err := c.simulator.RunNode(raftConf, engine, context.TODO())
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) Shutdown() {
	for _, nodeID := range c.simulator.GetNodeIds() {
		c.simulator.StopNode(nodeID)
	}
	for _, dir := range c.dirs {
		os.RemoveAll(dir)
	}
}

func (c *Cluster) AddFilter(filter Filter) {
	c.simulator.AddFilter(filter)
}

func (c *Cluster) ClearFilters() {
	c.simulator.ClearFilters()
}

func (c *Cluster) Request(key []byte, reqs []*raft_cmdpb.Request, readQuorum bool, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	region := c.GetRegion(key)
	regionID := region.GetId()
	req := NewRequest(regionID, region.RegionEpoch, reqs, readQuorum)
	for i := 0; i < 10; i++ {
		resp := c.CallCommandOnLeader(&req, timeout)
		if resp == nil || resp.Header != nil &&
			resp.Header.Error != nil &&
			(resp.Header.Error.GetEpochNotMatch() != nil || strings.Contains(resp.Header.Error.Message, "merging mode")) {
			SleepMS(100)
			continue
		}
		return resp
	}
	panic("request timeout")
}

func (c *Cluster) CallCommand(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	nodeID := request.Header.Peer.StoreId
	return c.simulator.CallCommandOnNode(nodeID, request, timeout)
}

func (c *Cluster) CallCommandOnLeader(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	startTime := time.Now()
	regionID := request.Header.RegionId
	leader := c.LeaderOfRegion(regionID)
	for {
		if time.Now().Sub(startTime) > timeout {
			log.Panicf("can't call command %s on leader of region %d", request.String(), regionID)
			return nil
		}
		request.Header.Peer = leader
		resp := c.CallCommand(request, timeout)
		if resp != nil && resp.Header != nil && resp.Header.Error != nil {
			err := resp.Header.Error
			if err.StaleCommand != nil ||
				err.EpochNotMatch != nil ||
				err.NotLeader != nil {
				log.Printf("warn: call command %s on leader of region %d errored (%s), retrying", request.String(), regionID, err.String())
				if err.NotLeader != nil &&
					err.NotLeader.Leader != nil {
					leader = err.NotLeader.Leader
				} else {
					leader = c.LeaderOfRegion(regionID)
				}
				continue
			}
		}
		return resp
	}
}

func (c *Cluster) LeaderOfRegion(regionID uint64) *metapb.Peer {
	for i := 0; i < 500; i++ {
		_, leader, err := c.pdClient.GetRegionByID(context.TODO(), regionID)
		if err == nil && leader != nil {
			log.Printf("leader of region: %d", leader.GetStoreId())
			return leader
		}
		SleepMS(10)
	}
	log.Panicf("can't get leader of region %d", regionID)
	return nil
}

func (c *Cluster) QueryLeader(storeID, regionID uint64, timeout time.Duration) *metapb.Peer {
	// To get region leader, we don't care real peer id, so use 0 instead.
	peer := NewPeer(storeID, 0)
	findLeader := NewStatusRequest(regionID, &peer, NewRegionLeaderCmd())
	resp := c.CallCommand(findLeader, timeout)
	if resp == nil {
		log.Panicf("fail to get leader of region %d on store %d", regionID, storeID)
	}
	regionLeader := resp.StatusResponse.RegionLeader
	if regionLeader != nil && c.ValidLeaderID(regionID, regionLeader.Leader.StoreId) {
		return regionLeader.Leader
	}
	return nil
}

func (c *Cluster) ValidLeaderID(regionID, leaderID uint64) bool {
	storeIds := c.GetStoreIdsOfRegion(regionID)
	for _, storeID := range storeIds {
		if leaderID == storeID {
			return true
		}
	}
	return false
}

func (c *Cluster) GetRegion(key []byte) *metapb.Region {
	for i := 0; i < 100; i++ {
		region, _, _ := c.pdClient.GetRegion(context.TODO(), key)
		if region != nil {
			return region
		}
		// We may meet range gap after split, so here we will
		// retry to get the region again.
		SleepMS(20)
	}
	log.Panicf("find no region for %s", hex.EncodeToString(key))
	return nil
}

func (c *Cluster) GetStoreIdsOfRegion(regionID uint64) []uint64 {
	region, _, err := c.pdClient.GetRegionByID(context.TODO(), regionID)
	if err != nil {
		panic(err)
	}
	peers := region.GetPeers()
	storeIds := make([]uint64, len(peers))
	for i, peer := range peers {
		storeIds[i] = peer.GetStoreId()
	}
	return storeIds
}

func (c *Cluster) MustPut(key, value []byte) {
	c.MustPutCF(engine_util.CfDefault, key, value)
}

func (c *Cluster) MustPutCF(cf string, key, value []byte) {
	req := NewPutCfCmd(cf, key, value)
	resp := c.Request(key, []*raft_cmdpb.Request{req}, false, 5*time.Second)
	if resp.Header.Error != nil {
		panic(resp.Header.Error)
	}
	if len(resp.Responses) != 1 {
		panic("len(resp.Responses) != 1")
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Put {
		panic("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Put")
	}
}

func (c *Cluster) Get(key []byte) []byte {
	return c.getImpl(engine_util.CfDefault, key, false)
}

func (c *Cluster) MustGet(key []byte) []byte {
	return c.getImpl(engine_util.CfDefault, key, true)
}

func (c *Cluster) getImpl(cf string, key []byte, readQuorum bool) []byte {
	req := NewGetCfCmd(cf, key)
	resp := c.Request(key, []*raft_cmdpb.Request{req}, readQuorum, 5*time.Second)
	if resp.Header.Error != nil {
		panic(resp.Header.Error)
	}
	if len(resp.Responses) != 1 {
		panic("len(resp.Responses) != 1")
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Get {
		panic("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Get")
	}
	return resp.Responses[0].Get.Value
}

func (c *Cluster) TransferLeader(regionID uint64, leader *metapb.Peer) {
	region, _, err := c.pdClient.GetRegionByID(context.TODO(), regionID)
	if err != nil {
		panic(err)
	}
	epoch := region.RegionEpoch
	transferLeader := NewAdminRequest(regionID, epoch, NewTransferLeaderCmd(leader))
	resp := c.CallCommandOnLeader(transferLeader, 5*time.Second)
	if resp.AdminResponse.CmdType != raft_cmdpb.AdminCmdType_TransferLeader {
		panic("resp.AdminResponse.CmdType != raft_cmdpb.AdminCmdType_TransferLeader")
	}
}

func (c *Cluster) MustTransferLeader(regionID uint64, leader *metapb.Peer) {
	timer := time.Now()
	for {
		currentLeader := c.LeaderOfRegion(regionID)
		if currentLeader.Id == leader.Id &&
			currentLeader.StoreId == leader.StoreId {
			return
		}
		if time.Now().Sub(timer) > 5*time.Second {
			log.Panicf("failed to transfer leader to [%d] %s", regionID, leader.String())
		}
		c.TransferLeader(regionID, leader)
	}
}

func (c *Cluster) Partition(s1 []uint64, s2 []uint64) {
	filter := &PartitionFilter{
		s1: s1,
		s2: s2,
	}
	c.simulator.AddFilter(filter)
}
