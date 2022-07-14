package raftstore

import (
	"context"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap/errors"
)

type Node struct {
	clusterID       uint64
	store           *metapb.Store
	cfg             *config.Config
	system          *Raftstore
	schedulerClient scheduler_client.Client
}

func NewNode(system *Raftstore, cfg *config.Config, schedulerClient scheduler_client.Client) *Node {
	return &Node{
		clusterID: schedulerClient.GetClusterID((context.TODO())),
		store: &metapb.Store{
			Address: cfg.StoreAddr,
		},
		cfg:             cfg,
		system:          system,
		schedulerClient: schedulerClient,
	}
}

func (n *Node) Start(ctx context.Context, engines *engine_util.Engines, trans Transport, snapMgr *snap.SnapManager) error {
	storeID, err := n.checkStore(engines)
	if err != nil {
		return err
	}
	if storeID == util.InvalidID {
		storeID, err = n.bootstrapStore(ctx, engines)
	}
	if err != nil {
		return err
	}
	n.store.Id = storeID

	firstRegion, err := n.checkOrPrepareBootstrapCluster(ctx, engines, storeID)
	if err != nil {
		return err
	}
	newCluster := firstRegion != nil
	if newCluster {
		log.Infof("try bootstrap cluster, storeID: %d, region: %s", storeID, firstRegion)
		newCluster, err = n.BootstrapCluster(ctx, engines, firstRegion)
		if err != nil {
			return err
		}
	}

	err = n.schedulerClient.PutStore(ctx, n.store)
	if err != nil {
		return err
	}
	if err = n.startNode(engines, trans, snapMgr); err != nil {
		return err
	}

	return nil
}

func (n *Node) checkStore(engines *engine_util.Engines) (uint64, error) {
	ident := new(raft_serverpb.StoreIdent)
	err := engine_util.GetMeta(engines.Kv, meta.StoreIdentKey, ident)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	if ident.ClusterId != n.clusterID {
		return 0, errors.Errorf("cluster ID mismatch, local %d != remote %d", ident.ClusterId, n.clusterID)
	}

	if ident.StoreId == util.InvalidID {
		return 0, errors.Errorf("invalid store ident %s", ident)
	}
	return ident.StoreId, nil
}

func (n *Node) bootstrapStore(ctx context.Context, engines *engine_util.Engines) (uint64, error) {
	storeID, err := n.allocID(ctx)
	if err != nil {
		return 0, err
	}
	err = BootstrapStore(engines, n.clusterID, storeID)
	return storeID, err
}

func (n *Node) allocID(ctx context.Context) (uint64, error) {
	return n.schedulerClient.AllocID(ctx)
}

func (n *Node) checkOrPrepareBootstrapCluster(ctx context.Context, engines *engine_util.Engines, storeID uint64) (*metapb.Region, error) {
	var state raft_serverpb.RegionLocalState
	if err := engine_util.GetMeta(engines.Kv, meta.PrepareBootstrapKey, &state); err == nil {
		return state.Region, nil
	}
	bootstrapped, err := n.checkClusterBootstrapped(ctx)
	if err != nil {
		return nil, err
	}
	if bootstrapped {
		return nil, nil
	}
	return n.prepareBootstrapCluster(ctx, engines, storeID)
}

const (
	MaxCheckClusterBootstrappedRetryCount = 60
	CheckClusterBootstrapRetrySeconds     = 3
)

func (n *Node) checkClusterBootstrapped(ctx context.Context) (bool, error) {
	for i := 0; i < MaxCheckClusterBootstrappedRetryCount; i++ {
		bootstrapped, err := n.schedulerClient.IsBootstrapped(ctx)
		if err == nil {
			return bootstrapped, nil
		}
		log.Warnf("check cluster bootstrapped failed, err: %v", err)
		time.Sleep(time.Second * CheckClusterBootstrapRetrySeconds)
	}
	return false, errors.New("check cluster bootstrapped failed")
}

func (n *Node) prepareBootstrapCluster(ctx context.Context, engines *engine_util.Engines, storeID uint64) (*metapb.Region, error) {
	regionID, err := n.allocID(ctx)
	if err != nil {
		return nil, err
	}
	log.Infof("alloc first region id, regionID: %d, clusterID: %d, storeID: %d", regionID, n.clusterID, storeID)
	peerID, err := n.allocID(ctx)
	if err != nil {
		return nil, err
	}
	log.Infof("alloc first peer id for first region, peerID: %d, regionID: %d", peerID, regionID)

	return PrepareBootstrap(engines, storeID, regionID, peerID)
}

func (n *Node) BootstrapCluster(ctx context.Context, engines *engine_util.Engines, firstRegion *metapb.Region) (newCluster bool, err error) {
	regionID := firstRegion.GetId()
	for retry := 0; retry < MaxCheckClusterBootstrappedRetryCount; retry++ {
		if retry != 0 {
			time.Sleep(time.Second)
		}

		res, err := n.schedulerClient.Bootstrap(ctx, n.store)
		if err != nil {
			log.Errorf("bootstrap cluster failed, clusterID: %d, err: %v", n.clusterID, err)
			continue
		}
		resErr := res.GetHeader().GetError()
		if resErr == nil {
			log.Infof("bootstrap cluster ok, clusterID: %d", n.clusterID)
			return true, ClearPrepareBootstrapState(engines)
		}
		if resErr.GetType() == schedulerpb.ErrorType_ALREADY_BOOTSTRAPPED {
			region, _, err := n.schedulerClient.GetRegion(ctx, []byte{})
			if err != nil {
				log.Errorf("get first region failed, err: %v", err)
				continue
			}
			if region.GetId() == regionID {
				return false, ClearPrepareBootstrapState(engines)
			}
			log.Infof("cluster is already bootstrapped, clusterID: %v", n.clusterID)
			return false, ClearPrepareBootstrap(engines, regionID)
		}
		log.Errorf("bootstrap cluster, clusterID: %v, err: %v", n.clusterID, resErr)
	}
	return false, errors.New("bootstrap cluster failed")
}

func (n *Node) startNode(engines *engine_util.Engines, trans Transport, snapMgr *snap.SnapManager) error {
	log.Infof("start raft store node, storeID: %d", n.store.GetId())
	return n.system.start(n.store, n.cfg, engines, trans, n.schedulerClient, snapMgr)
}

func (n *Node) stopNode(storeID uint64) {
	log.Infof("stop raft store thread, storeID: %d", storeID)
	n.system.shutDown()
}

func (n *Node) Stop() {
	n.stopNode(n.store.GetId())
}

func (n *Node) GetStoreID() uint64 {
	return n.store.GetId()
}

func (n *Node) GetDBPath() string {
	return n.cfg.DBPath
}
