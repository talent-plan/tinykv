package raftstore

import (
	"bytes"
	"sync"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

/// loadPeers loads peers in this store. It scans the db engine, loads all regions
/// and their peers from it, and schedules snapshot worker if necessary.
/// WARN: This store should not be used before initialized.
func (bs *RaftBatchSystem) loadPeers() ([]*peerFsm, error) {
	// Scan region meta to get saved regions.
	startKey := RegionMetaMinKey
	endKey := RegionMetaMaxKey
	ctx := bs.ctx
	kvEngine := ctx.engine.Kv
	storeID := ctx.store.Id

	var totalCount, tombStoneCount, applyingCount int
	var regionPeers []*peerFsm

	t := time.Now()
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	var applyingRegions []*metapb.Region
	var mergingCount int
	ctx.storeMetaLock.Lock()
	defer ctx.storeMetaLock.Unlock()
	meta := ctx.storeMeta
	err := kvEngine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := decodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}
			if localState.State == rspb.PeerState_Applying {
				// in case of restart happen when we just write region state to Applying,
				// but not write raft_local_state to raft rocksdb in time.
				applyingCount++
				applyingRegions = append(applyingRegions, region)
				continue
			}

			peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			meta.regionRanges.Insert(region.EndKey, regionIDToBytes(regionID))
			meta.regions[regionID] = region
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kvWB.MustWriteToDB(ctx.engine.Kv)
	raftWB.MustWriteToDB(ctx.engine.Raft)

	// schedule applying snapshot after raft write batch were written.
	for _, region := range applyingRegions {
		log.Infof("region %d is applying snapshot", region.Id)
		peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
		if err != nil {
			return nil, err
		}
		peer.scheduleApplyingSnapshot()
		meta.regionRanges.Insert(region.EndKey, regionIDToBytes(region.Id))
		meta.regions[region.Id] = region
		regionPeers = append(regionPeers, peer)
	}
	log.Infof("start store %d, region_count %d, tombstone_count %d, applying_count %d, merge_count %d, takes %v",
		storeID, totalCount, tombStoneCount, applyingCount, mergingCount, time.Since(t))
	return regionPeers, nil
}

func (bs *RaftBatchSystem) clearStaleMeta(kvWB, raftWB *engine_util.WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftKey := RaftStateKey(region.Id)
	raftState := raftState{}
	val, err := getValue(bs.ctx.engine.Raft, raftKey)
	if err != nil {
		// it has been cleaned up.
		return
	}
	raftState.Unmarshal(val)
	err = ClearMeta(bs.ctx.engine, kvWB, raftWB, region.Id, raftState.lastIndex)
	if err != nil {
		panic(err)
	}
	key := RegionStateKey(region.Id)
	if err := kvWB.SetMsg(key, originState); err != nil {
		panic(err)
	}
}

type workers struct {
	raftLogGCWorker  *worker.Worker
	pdWorker         *worker.Worker
	splitCheckWorker *worker.Worker
	regionWorker     *worker.Worker
	wg               *sync.WaitGroup
}

type RaftBatchSystem struct {
	ctx        *GlobalContext
	router     *router
	workers    *workers
	tickDriver *tickDriver
	closeCh    chan struct{}
	wg         *sync.WaitGroup
}

func (bs *RaftBatchSystem) start(
	meta *metapb.Store,
	cfg *config.Config,
	engines *engine_util.Engines,
	trans Transport,
	pdClient pd.Client,
	snapMgr *snap.SnapManager,
	pdWorker *worker.Worker) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	err := snapMgr.Init()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker: worker.NewWorker("split-check", wg),
		regionWorker:     worker.NewWorker("snapshot-worker", wg),
		raftLogGCWorker:  worker.NewWorker("raft-gc-worker", wg),
		pdWorker:         pdWorker,
		wg:               wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                  cfg,
		engine:               engines,
		store:                meta,
		storeMeta:            newStoreMeta(),
		storeMetaLock:        new(sync.RWMutex),
		snapMgr:              snapMgr,
		router:               bs.router,
		trans:                trans,
		pdTaskSender:         bs.workers.pdWorker.Sender(),
		regionTaskSender:     bs.workers.regionWorker.Sender(),
		splitCheckTaskSender: bs.workers.splitCheckWorker.Sender(),
		raftLogGCTaskSender:  bs.workers.raftLogGCWorker.Sender(),
		pdClient:             pdClient,
		tickDriverSender:     bs.tickDriver.newRegionCh,
	}
	regionPeers, err := bs.loadPeers()
	if err != nil {
		return err
	}

	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	bs.startWorkers(regionPeers)
	return nil
}

func (bs *RaftBatchSystem) startWorkers(peers []*peerFsm) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router
	for i := 0; i < ctx.cfg.RaftWorkerCnt; i++ {
		rw := newRaftWorker(ctx, router.workerSenders[i], router)
		bs.wg.Add(1)
		go rw.run(bs.closeCh, bs.wg)
	}
	storeCtx := &StoreContext{GlobalContext: ctx, applyingSnapCount: new(uint64)}
	sw := &storeWorker{
		store: newStoreFsmDelegate(router.storeFsm, storeCtx),
	}
	bs.wg.Add(1)
	go sw.run(bs.closeCh, bs.wg)
	router.sendStore(message.Msg{Type: message.MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].peer.regionId
		_ = router.send(regionID, message.Msg{RegionID: regionID, Type: message.MsgTypeStart})
	}
	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.Start(newSplitCheckHandler(engines.Kv, router, cfg.SplitCheck))
	workers.regionWorker.Start(newRegionTaskHandler(engines, ctx.snapMgr))
	workers.raftLogGCWorker.Start(&raftLogGCTaskHandler{})
	workers.pdWorker.Start(newPDTaskHandler(ctx.store.Id, ctx.pdClient, NewRaftstoreRouter(bs.router)))
	bs.wg.Add(1)
	go bs.tickDriver.run(bs.closeCh, bs.wg) // TODO: temp workaround.
}

func (bs *RaftBatchSystem) shutDown() {
	if bs.workers == nil {
		return
	}
	close(bs.closeCh)
	bs.wg.Wait()
	workers := bs.workers
	bs.workers = nil
	stopTask := worker.Task{Tp: worker.TaskTypeStop}
	workers.splitCheckWorker.Sender() <- stopTask
	workers.regionWorker.Sender() <- stopTask
	workers.raftLogGCWorker.Sender() <- stopTask
	workers.pdWorker.Sender() <- stopTask
	workers.wg.Wait()
}

func CreateRaftBatchSystem(cfg *config.Config) (*router, *RaftBatchSystem) {
	storeSender, storeFsm := newStoreFsm(cfg)
	router := newRouter(cfg.RaftWorkerCnt, storeSender, storeFsm)
	raftBatchSystem := &RaftBatchSystem{
		router:     router,
		tickDriver: newTickDriver(cfg.RaftBaseTickInterval, router, storeFsm.ticker),
		closeCh:    make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
	return router, raftBatchSystem
}
