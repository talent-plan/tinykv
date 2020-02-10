package test_raftstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	tikvConf "github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type Filter interface {
	Before(msgs *raft_serverpb.RaftMessage) bool
	After()
}

type Simulator interface {
	RunNode(raftConf *tikvConf.Config, engine *engine_util.Engines, ctx context.Context) error
	StopNode(nodeID uint64)
	AddSendFilter(nodeID uint64, filter Filter)
	AddReceiveFilter(nodeID uint64, filter Filter)
	ClearSendFilters(nodeID uint64)
	ClearReceiveFilters(nodeID uint64)
	GetNodeIds() []uint64
	CallCommandOnNode(nodeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse
}

type Cluster struct {
	pdClient  pd.Client
	context   context.Context
	count     int
	engines   []*engine_util.Engines
	dirs      []string
	simulator Simulator
}

func NewCluster(count int, pdClient pd.Client, simulator Simulator) Cluster {
	return Cluster{
		count:     count,
		pdClient:  pdClient,
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
		c.dirs = []string{kvPath, raftPath, snapPath}

		err = os.MkdirAll(kvPath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		err = os.MkdirAll(raftPath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		err = os.MkdirAll(snapPath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}

		conf := config.DefaultConf
		conf.Engine.DBPath = dbPath

		raftDB := engine_util.CreateDB("raft", &conf.Engine)
		raftConf := tikvConf.NewDefaultConfig()
		raftConf.SnapPath = snapPath

		kvDB := engine_util.CreateDB("kv", &conf.Engine)
		engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
		err = raftstore.BootstrapStore(engine, clusterID, nodeID)
		if err != nil {
			return err
		}
		c.engines = append(c.engines, engine)

		err = c.simulator.RunNode(raftConf, engine, c.context)
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
	log.Fatal("request timeout")
	return nil
}

func (c *Cluster) CallCommand(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	nodeID := request.Header.Peer.StoreId
	return c.simulator.CallCommandOnNode(nodeID, request, timeout)
}

func (c *Cluster) CallCommandOnLeader(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	regionID := request.Header.RegionId
	leader := c.LeaderOfRegion(regionID)
	if leader == nil {
		log.Panicf("can't get leader of region %d", regionID)
	}
	request.Header.Peer = leader
	resp := c.CallCommand(request, timeout)
	if resp == nil {
		log.Panicf("can't call command %s on leader of region %d", request.String(), regionID)
	}
	return resp
}

func (c *Cluster) LeaderOfRegion(regionID uint64) *metapb.Peer {
	storeIds := c.GetStoreIdsOfRegion(regionID)
	for _, storeID := range storeIds {
		leader := c.QueryLeader(storeID, regionID, 1*time.Second)
		if leader != nil {
			return leader
		}
	}
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
	if c.ValidLeaderID(regionID, regionLeader.Leader.StoreId) {
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
		region, _, _ := c.pdClient.GetRegion(c.context, key)
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
	region, _, err := c.pdClient.GetRegionByID(c.context, regionID)
	if err != nil {
		log.Fatal(err)
	}
	peers := region.GetPeers()
	storeIds := make([]uint64, len(peers))
	for i, peer := range peers {
		storeIds[i] = peer.GetStoreId()
	}
	return storeIds
}

func (c *Cluster) MustPut(key, value []byte) {
	c.MustPutCF("default", key, value)
}

func (c *Cluster) MustPutCF(cf string, key, value []byte) {
	req := NewPutCfCmd("cf", key, value)
	resp := c.Request(key, []*raft_cmdpb.Request{req}, false, 5*time.Second)
	if resp.Header.Error != nil {
		log.Fatal(resp.Header.Error)
	}
	if len(resp.Responses) != 1 {
		log.Fatal("len(resp.Responses) != 1")
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Put {
		log.Fatal("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Put")
	}
}

// MustGet value is optional
func (c *Cluster) MustGet(engineID uint, cf string, key []byte, value []byte) {
	engine := c.engines[engineID]
	if engine == nil {
		log.Panicf("can not find engine with ID %d", engineID)
	}
	val, err := engine_util.GetCF(engine.Kv, cf, key)
	if err != nil {
		log.Fatal(err)
	}
	if value != nil && bytes.Compare(val, value) != 0 {
		log.Panicf("can't get value %s for key %s", hex.EncodeToString(value), hex.EncodeToString(key))
	}
}

func (c *Cluster) MustGetEqual(engineID uint, key []byte, value []byte) {
	c.MustGet(engineID, "default", key, value)
}
