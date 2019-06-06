package raftstore

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/coocood/badger"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type Node struct {
	clusterID uint64
	store     *metapb.Store
	cfg       *Config
	storeWg   *sync.WaitGroup
	system    *raftBatchSystem
	pdClient  pd.Client
	observer  PeerEventObserver
}

func NewNode(system *raftBatchSystem, cfg *Config, pdClient pd.Client, observer PeerEventObserver) *Node {
	var store metapb.Store
	if cfg.AdvertiseAddr != "" {
		store.Address = cfg.AdvertiseAddr
	} else {
		store.Address = cfg.Addr
	}
	store.Version = os.Getenv("TIKV_VERSION") // FIXME: Arbitrarily use an environment variable now.
	for _, l := range cfg.Labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: l.LabelKey, Value: l.LabelValue})
	}
	return &Node{
		clusterID: pdClient.GetClusterID((context.TODO())),
		store:     &store,
		cfg:       cfg,
		storeWg:   &sync.WaitGroup{},
		system:    system,
		pdClient:  pdClient,
		observer:  observer,
	}
}

func (n *Node) Start(ctx context.Context, engines *Engines, trans Transport, snapMgr *SnapManager, pdWorker *worker, copHost *CoprocessorHost) error {
	storeID, err := n.checkStore(engines)
	if err != nil {
		return err
	}
	if storeID == InvalidID {
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
	log.Infof("try bootstrap cluster, storeID: %d, region: %s", storeID, firstRegion)
	err = n.BootstrapCluster(ctx, engines, firstRegion)
	if err != nil {
		return err
	}
	err = n.pdClient.PutStore(ctx, n.store)
	if err != nil {
		return err
	}
	err = n.startNode(engines, trans, snapMgr, pdWorker, copHost)
	return err
}

func (n *Node) checkStore(engines *Engines) (uint64, error) {
	val, err := getValue(engines.kv.db, storeIdentKey)
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

	if ident.StoreId == InvalidID {
		return 0, errors.Errorf("invalid store ident %s", &ident)
	}
	return ident.StoreId, nil
}

func (n *Node) bootstrapStore(ctx context.Context, engines *Engines) (uint64, error) {
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

func (n *Node) checkOrPrepareBootstrapCluster(ctx context.Context, engines *Engines, storeID uint64) (*metapb.Region, error) {
	var state raft_serverpb.RegionLocalState
	if err := getMsg(engines.kv.db, prepareBootstrapKey, &state); err == nil {
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

func (n *Node) prepareBootstrapCluster(ctx context.Context, engines *Engines, storeID uint64) (*metapb.Region, error) {
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

func (n *Node) BootstrapCluster(ctx context.Context, engines *Engines, firstRegion *metapb.Region) error {
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
			return ClearPrepareBootstrapState(engines)
		}
		if resErr.GetType() == pdpb.ErrorType_ALREADY_BOOTSTRAPPED {
			region, err := n.pdClient.GetRegion(ctx, []byte{})
			if err != nil {
				log.Errorf("get first region failed, err: %v", err)
				continue
			}
			if region.GetId() == regionID {
				return ClearPrepareBootstrapState(engines)
			}
			log.Infof("cluster is already bootstrapped, clusterID: %v", n.clusterID)
			return ClearPrepareBootstrap(engines, regionID)
		}
		log.Errorf("bootstrap cluster, clusterID: %v, err: %v", n.clusterID, err)
	}
	return errors.New("bootstrap cluster failed")
}

func (n *Node) startNode(engines *Engines, trans Transport, snapMgr *SnapManager, pdWorker *worker, copHost *CoprocessorHost) error {
	log.Infof("start raft store node, storeID: %d", n.store.GetId())
	return n.system.start(n.store, n.cfg, engines, trans, n.pdClient, snapMgr, pdWorker, copHost, n.observer)
}

func (n *Node) stopNode(storeID uint64) {
	log.Infof("stop raft store thread, storeID: %d", storeID)
	n.system.shutDown()
}

func (n *Node) stop() {
	n.stopNode(n.store.GetId())
}
