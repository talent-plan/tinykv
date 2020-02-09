package raftstore

import (
	"context"
	"sync"
	"time"

	"github.com/coocood/badger"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

type Node struct {
	clusterID uint64
	store     *metapb.Store
	cfg       *config.Config
	storeWg   *sync.WaitGroup
	system    *RaftBatchSystem
	pdClient  pd.Client
}

func NewNode(system *RaftBatchSystem, store *metapb.Store, cfg *config.Config, pdClient pd.Client) *Node {
	if cfg.AdvertiseAddr != "" {
		store.Address = cfg.AdvertiseAddr
	} else {
		store.Address = cfg.Addr
	}
	store.Version = "3.0.0-bata.1"
	for _, l := range cfg.Labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: l.LabelKey, Value: l.LabelValue})
	}
	return &Node{
		clusterID: pdClient.GetClusterID((context.TODO())),
		store:     store,
		cfg:       cfg,
		storeWg:   &sync.WaitGroup{},
		system:    system,
		pdClient:  pdClient,
	}
}

func (n *Node) Start(ctx context.Context, engines *engine_util.Engines, trans Transport, snapMgr *snap.SnapManager, pdWorker *worker.Worker, router *RaftstoreRouter) error {
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

	err = n.pdClient.PutStore(ctx, n.store)
	if err != nil {
		return err
	}
	if err = n.startNode(engines, trans, snapMgr, pdWorker); err != nil {
		return err
	}

	return nil
}

func (n *Node) checkStore(engines *engine_util.Engines) (uint64, error) {
	val, err := engine_util.GetValue(engines.Kv, meta.StoreIdentKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}

	var ident raft_serverpb.StoreIdent
	err = proto.Unmarshal(val, &ident)
	if err != nil {
		return 0, err
	}

	if ident.ClusterId != n.clusterID {
		return 0, errors.Errorf("cluster ID mismatch, local %d != remote %d", ident.ClusterId, n.clusterID)
	}

	if ident.StoreId == util.InvalidID {
		return 0, errors.Errorf("invalid store ident %s", &ident)
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
	return n.pdClient.AllocID(ctx)
}

func (n *Node) checkOrPrepareBootstrapCluster(ctx context.Context, engines *engine_util.Engines, storeID uint64) (*metapb.Region, error) {
	var state raft_serverpb.RegionLocalState
	if err := engine_util.GetMsg(engines.Kv, meta.PrepareBootstrapKey, &state); err == nil {
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
		bootstrapped, err := n.pdClient.IsBootstrapped(ctx)
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

		res, err := n.pdClient.Bootstrap(ctx, n.store, firstRegion)
		if err != nil {
			log.Errorf("bootstrap cluster failed, clusterID: %d, err: %v", n.clusterID, err)
			continue
		}
		resErr := res.GetHeader().GetError()
		if resErr == nil {
			log.Infof("bootstrap cluster ok, clusterID: %d", n.clusterID)
			return true, ClearPrepareBootstrapState(engines)
		}
		if resErr.GetType() == pdpb.ErrorType_ALREADY_BOOTSTRAPPED {
			region, _, err := n.pdClient.GetRegion(ctx, []byte{})
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
		log.Errorf("bootstrap cluster, clusterID: %v, err: %v", n.clusterID, err)
	}
	return false, errors.New("bootstrap cluster failed")
}

func (n *Node) startNode(engines *engine_util.Engines, trans Transport, snapMgr *snap.SnapManager, pdWorker *worker.Worker) error {
	log.Infof("start raft store node, storeID: %d", n.store.GetId())
	return n.system.start(n.store, n.cfg, engines, trans, n.pdClient, snapMgr, pdWorker)
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
