package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// peerState contains the peer states that needs to run raft command and apply command.
// It binds to a worker to make sure the commands are always executed on a same goroutine.
type peerState struct {
	closed uint32
	peer   *peerFsm
	apply  *applier
}

type applyBatch struct {
	msgs      []message.Msg
	proposals []*regionProposal
}

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	raftCh  chan message.Msg
	raftCtx *RaftContext

	applyCh chan *applyBatch

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	raftCtx := &RaftContext{
		GlobalContext: ctx,
		applyMsgs:     new(applyMsgs),
		queuedSnaps:   make(map[uint64]struct{}),
		kvWB:          new(engine_util.WriteBatch),
		raftWB:        new(engine_util.WriteBatch),
	}
	return &raftWorker{
		raftCh:  pm.peerSender,
		raftCtx: raftCtx,
		applyCh: make(chan *applyBatch, 1),
		pr:      pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			rw.applyCh <- nil
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		rw.raftCtx.pendingCount = 0
		rw.raftCtx.hasReady = false
		batch := &applyBatch{}
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			newRaftMsgHandler(peerState.peer, rw.raftCtx).HandleMsgs(msg)
		}
		for _, peerState := range peerStateMap {
			batch.proposals = newRaftMsgHandler(peerState.peer, rw.raftCtx).HandleRaftReadyAppend(batch.proposals)
		}
		if rw.raftCtx.hasReady {
			rw.handleRaftReady(peerStateMap, batch)
		}
		applyMsgs := rw.raftCtx.applyMsgs
		batch.msgs = append(batch.msgs, applyMsgs.msgs...)
		applyMsgs.msgs = applyMsgs.msgs[:0]
		rw.removeQueuedSnapshots()
		rw.applyCh <- batch
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}

func (rw *raftWorker) handleRaftReady(peers map[uint64]*peerState, batch *applyBatch) {
	for _, proposal := range batch.proposals {
		msg := message.Msg{Type: message.MsgTypeApplyProposal, Data: proposal}
		rw.raftCtx.applyMsgs.appendMsg(proposal.RegionId, msg)
	}
	kvWB := rw.raftCtx.kvWB
	kvWB.MustWriteToDB(rw.raftCtx.engine.Kv)
	kvWB.Reset()
	raftWB := rw.raftCtx.raftWB
	raftWB.MustWriteToDB(rw.raftCtx.engine.Raft)
	raftWB.Reset()
	readyRes := rw.raftCtx.ReadyRes
	rw.raftCtx.ReadyRes = nil
	if len(readyRes) > 0 {
		for _, pair := range readyRes {
			regionID := pair.IC.RegionID
			newRaftMsgHandler(peers[regionID].peer, rw.raftCtx).PostRaftReadyPersistent(&pair.Ready, pair.IC)
		}
	}
}

func (rw *raftWorker) removeQueuedSnapshots() {
	if len(rw.raftCtx.queuedSnaps) > 0 {
		meta := rw.raftCtx.storeMeta
		retained := meta.pendingSnapshotRegions[:0]
		for _, region := range meta.pendingSnapshotRegions {
			if _, ok := rw.raftCtx.queuedSnaps[region.Id]; !ok {
				retained = append(retained, region)
			}
		}
		meta.pendingSnapshotRegions = retained
		rw.raftCtx.queuedSnaps = map[uint64]struct{}{}
	}
}

type applyWorker struct {
	pr       *router
	applyCh  chan *applyBatch
	applyCtx *applyContext
}

func newApplyWorker(ctx *GlobalContext, ch chan *applyBatch, pr *router) *applyWorker {
	return &applyWorker{
		pr:       pr,
		applyCh:  ch,
		applyCtx: newApplyContext("", ctx.engine, pr.peerSender, ctx.cfg),
	}
}

// run runs apply tasks, since it is already batched by raftCh, we don't need to batch it here.
func (aw *applyWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		batch := <-aw.applyCh
		if batch == nil {
			return
		}
		for _, msg := range batch.msgs {
			ps := aw.pr.get(msg.RegionID)
			if ps == nil {
				// TODO: figure out a way to invoke all cmd for the deleted applier
				continue
			}
			ps.apply.handleTask(aw.applyCtx, msg)
		}
		aw.applyCtx.flush()
	}
}

// storeWorker runs store commands.
type storeWorker struct {
	store *storeMsgHandler
}

func newStoreWorker(ctx *GlobalContext, r *router) *storeWorker {
	storeCtx := &StoreContext{GlobalContext: ctx, applyingSnapCount: new(uint64)}
	return &storeWorker{
		store: newStoreFsmDelegate(r.storeFsm, storeCtx),
	}
}

func (sw *storeWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		var msg message.Msg
		select {
		case <-closeCh:
			return
		case msg = <-sw.store.receiver:
		}
		sw.store.handleMsg(msg)
	}
}
