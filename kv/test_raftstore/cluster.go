package test_raftstore

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type Simulator interface {
	RunStore(raftConf *config.Config, engine *engine_util.Engines, ctx context.Context) error
	StopStore(storeID uint64)
	AddFilter(filter Filter)
	ClearFilters()
	GetStoreIds() []uint64
	CallCommandOnStore(storeID uint64, request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn)
}

type Cluster struct {
	schedulerClient *MockSchedulerClient
	count           int
	engines         map[uint64]*engine_util.Engines
	snapPaths       map[uint64]string
	dirs            []string
	simulator       Simulator
	cfg             *config.Config
	baseDir         string
}

func NewCluster(count int, schedulerClient *MockSchedulerClient, simulator Simulator, cfg *config.Config) *Cluster {
	return &Cluster{
		count:           count,
		schedulerClient: schedulerClient,
		engines:         make(map[uint64]*engine_util.Engines),
		snapPaths:       make(map[uint64]string),
		simulator:       simulator,
		cfg:             cfg,
		baseDir:         "test-raftstore",
	}
}

func (c *Cluster) Start() {
	ctx := context.TODO()
	clusterID := c.schedulerClient.GetClusterID(ctx)

	for storeID := uint64(1); storeID <= uint64(c.count); storeID++ {
		dbPath, err := ioutil.TempDir("", c.baseDir)
		if err != nil {
			panic(err)
		}
		c.cfg.DBPath = dbPath
		kvPath := filepath.Join(dbPath, "kv")
		raftPath := filepath.Join(dbPath, "raft")
		snapPath := filepath.Join(dbPath, "snap")
		c.snapPaths[storeID] = snapPath
		c.dirs = append(c.dirs, dbPath)

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

		raftDB := engine_util.CreateDB(raftPath, true)
		kvDB := engine_util.CreateDB(kvPath, false)
		engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
		c.engines[storeID] = engine
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

	for storeID, engine := range c.engines {
		peer := NewPeer(storeID, storeID)
		firstRegion.Peers = append(firstRegion.Peers, peer)
		err := raftstore.BootstrapStore(engine, clusterID, storeID)
		if err != nil {
			panic(err)
		}
	}

	for _, engine := range c.engines {
		raftstore.PrepareBootstrapCluster(engine, firstRegion)
	}

	store := &metapb.Store{
		Id:      1,
		Address: "",
	}
	resp, err := c.schedulerClient.Bootstrap(context.TODO(), store)
	if err != nil {
		panic(err)
	}
	if resp.Header != nil && resp.Header.Error != nil {
		panic(resp.Header.Error)
	}

	for storeID, engine := range c.engines {
		store := &metapb.Store{
			Id:      storeID,
			Address: "",
		}
		err := c.schedulerClient.PutStore(context.TODO(), store)
		if err != nil {
			panic(err)
		}
		raftstore.ClearPrepareBootstrapState(engine)
	}

	for storeID := range c.engines {
		c.StartServer(storeID)
	}
}

func (c *Cluster) Shutdown() {
	for _, storeID := range c.simulator.GetStoreIds() {
		c.simulator.StopStore(storeID)
	}
	for _, engine := range c.engines {
		engine.Close()
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

func (c *Cluster) StopServer(storeID uint64) {
	c.simulator.StopStore(storeID)
}

func (c *Cluster) StartServer(storeID uint64) {
	engine := c.engines[storeID]
	err := c.simulator.RunStore(c.cfg, engine, context.TODO())
	if err != nil {
		panic(err)
	}
}

func (c *Cluster) AllocPeer(storeID uint64) *metapb.Peer {
	id, err := c.schedulerClient.AllocID(context.TODO())
	if err != nil {
		panic(err)
	}
	return NewPeer(storeID, id)
}

func (c *Cluster) Request(key []byte, reqs []*raft_cmdpb.Request, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	startTime := time.Now()
	for i := 0; i < 10 || time.Since(startTime) < timeout; i++ {
		region := c.GetRegion(key)
		regionID := region.GetId()
		req := NewRequest(regionID, region.RegionEpoch, reqs)
		resp, txn := c.CallCommandOnLeader(&req, timeout)
		if resp == nil {
			// it should be timeouted innerly
			SleepMS(100)
			continue
		}
		if resp.Header.Error != nil {
			SleepMS(100)
			continue
		}
		return resp, txn
	}
	panic("request timeout")
}

func (c *Cluster) CallCommand(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	storeID := request.Header.Peer.StoreId
	return c.simulator.CallCommandOnStore(storeID, request, timeout)
}

func (c *Cluster) CallCommandOnLeader(request *raft_cmdpb.RaftCmdRequest, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	startTime := time.Now()
	regionID := request.Header.RegionId
	leader := c.LeaderOfRegion(regionID)
	for {
		if time.Since(startTime) > timeout {
			return nil, nil
		}
		if leader == nil {
			panic(fmt.Sprintf("can't get leader of region %d", regionID))
		}
		request.Header.Peer = leader
		resp, txn := c.CallCommand(request, 1*time.Second)
		if resp == nil {
			log.Debugf("can't call command %s on leader %d of region %d", request.String(), leader.GetId(), regionID)
			newLeader := c.LeaderOfRegion(regionID)
			if leader == newLeader {
				region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
				if err != nil {
					return nil, nil
				}
				peers := region.GetPeers()
				leader = peers[rand.Int()%len(peers)]
				log.Debugf("leader info maybe wrong, use random leader %d of region %d", leader.GetId(), regionID)
			} else {
				leader = newLeader
				log.Debugf("use new leader %d of region %d", leader.GetId(), regionID)
			}
			continue
		}
		if resp.Header.Error != nil {
			err := resp.Header.Error
			if err.GetStaleCommand() != nil || err.GetEpochNotMatch() != nil || err.GetNotLeader() != nil {
				log.Debugf("encouter retryable err %+v", resp)
				if err.GetNotLeader() != nil && err.GetNotLeader().Leader != nil {
					leader = err.GetNotLeader().Leader
				} else {
					leader = c.LeaderOfRegion(regionID)
				}
				continue
			}
		}
		return resp, txn
	}
}

func (c *Cluster) LeaderOfRegion(regionID uint64) *metapb.Peer {
	for i := 0; i < 500; i++ {
		_, leader, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err == nil && leader != nil {
			return leader
		}
		SleepMS(10)
	}
	return nil
}

func (c *Cluster) GetRegion(key []byte) *metapb.Region {
	for i := 0; i < 100; i++ {
		region, _, _ := c.schedulerClient.GetRegion(context.TODO(), key)
		if region != nil {
			return region
		}
		// We may meet range gap after split, so here we will
		// retry to get the region again.
		SleepMS(20)
	}
	panic(fmt.Sprintf("find no region for %s", key))
}

func (c *Cluster) GetRandomRegion() *metapb.Region {
	return c.schedulerClient.getRandomRegion()
}

func (c *Cluster) GetStoreIdsOfRegion(regionID uint64) []uint64 {
	region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
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
	resp, _ := c.Request(key, []*raft_cmdpb.Request{req}, 5*time.Second)
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

func (c *Cluster) MustGet(key []byte, value []byte) {
	v := c.Get(key)
	if !bytes.Equal(v, value) {
		panic(fmt.Sprintf("expected value %s, but got %s", value, v))
	}
}

func (c *Cluster) Get(key []byte) []byte {
	return c.GetCF(engine_util.CfDefault, key)
}

func (c *Cluster) GetCF(cf string, key []byte) []byte {
	req := NewGetCfCmd(cf, key)
	resp, _ := c.Request(key, []*raft_cmdpb.Request{req}, 5*time.Second)
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

func (c *Cluster) MustDelete(key []byte) {
	c.MustDeleteCF(engine_util.CfDefault, key)
}

func (c *Cluster) MustDeleteCF(cf string, key []byte) {
	req := NewDeleteCfCmd(cf, key)
	resp, _ := c.Request(key, []*raft_cmdpb.Request{req}, 5*time.Second)
	if resp.Header.Error != nil {
		panic(resp.Header.Error)
	}
	if len(resp.Responses) != 1 {
		panic("len(resp.Responses) != 1")
	}
	if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Delete {
		panic("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Delete")
	}
}

func (c *Cluster) Scan(start, end []byte) [][]byte {
	req := NewSnapCmd()
	values := make([][]byte, 0)
	key := start
	for (len(end) != 0 && bytes.Compare(key, end) < 0) || (len(key) == 0 && len(end) == 0) {
		resp, txn := c.Request(key, []*raft_cmdpb.Request{req}, 5*time.Second)
		if resp.Header.Error != nil {
			panic(resp.Header.Error)
		}
		if len(resp.Responses) != 1 {
			panic("len(resp.Responses) != 1")
		}
		if resp.Responses[0].CmdType != raft_cmdpb.CmdType_Snap {
			panic("resp.Responses[0].CmdType != raft_cmdpb.CmdType_Snap")
		}
		region := resp.Responses[0].GetSnap().Region
		iter := raft_storage.NewRegionReader(txn, *region).IterCF(engine_util.CfDefault)
		for iter.Seek(key); iter.Valid(); iter.Next() {
			if engine_util.ExceedEndKey(iter.Item().Key(), end) {
				break
			}
			value, err := iter.Item().ValueCopy(nil)
			if err != nil {
				panic(err)
			}
			values = append(values, value)
		}
		iter.Close()

		key = region.EndKey
		if len(key) == 0 {
			break
		}
	}

	return values
}

func (c *Cluster) TransferLeader(regionID uint64, leader *metapb.Peer) {
	region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
	if err != nil {
		panic(err)
	}
	epoch := region.RegionEpoch
	transferLeader := NewAdminRequest(regionID, epoch, NewTransferLeaderCmd(leader))
	resp, _ := c.CallCommandOnLeader(transferLeader, 5*time.Second)
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
		if time.Since(timer) > 5*time.Second {
			panic(fmt.Sprintf("failed to transfer leader to [%d] %s", regionID, leader.String()))
		}
		c.TransferLeader(regionID, leader)
	}
}

func (c *Cluster) MustAddPeer(regionID uint64, peer *metapb.Peer) {
	c.schedulerClient.AddPeer(regionID, peer)
	c.MustHavePeer(regionID, peer)
}

func (c *Cluster) MustRemovePeer(regionID uint64, peer *metapb.Peer) {
	c.schedulerClient.RemovePeer(regionID, peer)
	c.MustNonePeer(regionID, peer)
}

func (c *Cluster) MustHavePeer(regionID uint64, peer *metapb.Peer) {
	for i := 0; i < 500; i++ {
		region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err != nil {
			panic(err)
		}
		if region != nil {
			if p := FindPeer(region, peer.GetStoreId()); p != nil {
				if p.GetId() == peer.GetId() {
					return
				}
			}
		}
		SleepMS(10)
	}
	panic(fmt.Sprintf("no peer: %v", peer))
}

func (c *Cluster) MustNonePeer(regionID uint64, peer *metapb.Peer) {
	for i := 0; i < 500; i++ {
		region, _, err := c.schedulerClient.GetRegionByID(context.TODO(), regionID)
		if err != nil {
			panic(err)
		}
		if region != nil {
			if p := FindPeer(region, peer.GetStoreId()); p != nil {
				if p.GetId() != peer.GetId() {
					return
				}
			} else {
				return
			}
		}
		SleepMS(10)
	}
	panic(fmt.Sprintf("have peer: %v", peer))
}
