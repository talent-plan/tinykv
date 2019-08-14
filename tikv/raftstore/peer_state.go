package raftstore

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/log"
)

// peerState contains the peer states that needs to run raft command and apply command.
// It binds to a worker to make sure the commands are always executed on a same goroutine.
type peerState struct {
	handle unsafe.Pointer
	peer   *peerFsm
	apply  *applyFsm
}

// changeWorker changes the worker binding.
// The workerHandle is immutable, when we need to update it, we create a new handle and do a CAS operation.
func (np *peerState) changeWorker(workerCh chan Msg) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	newHandle := &workerHandle{
		msgCh:   workerCh,
		barrier: wg,
	}
	oldHandle := (*workerHandle)(atomic.SwapPointer(&np.handle, unsafe.Pointer(newHandle)))
	// Sleep a little to make sure the barrier message is the last one for the peer on the old worker.
	time.Sleep(time.Millisecond)
	oldHandle.msgCh <- Msg{Type: MsgTypeBarrier, Data: wg}
}

func (np *peerState) send(msg Msg) error {
	for {
		handle := (*workerHandle)(atomic.LoadPointer(&np.handle))
		if handle.barrier != nil {
			// Newly bound worker, need to wait for old worker to finish all messages.
			handle.barrier.Wait()
			newHandle := &workerHandle{
				msgCh: handle.msgCh,
			}
			if !atomic.CompareAndSwapPointer(&np.handle, unsafe.Pointer(handle), unsafe.Pointer(newHandle)) {
				continue
			}
		}
		if handle.closed {
			return errMailboxNotFound
		}
		handle.msgCh <- msg
		return nil
	}
}

func (np *peerState) close() {
	closeHandle := &workerHandle{closed: true}
	atomic.StorePointer(&np.handle, unsafe.Pointer(closeHandle))
}

// workerHandle binds a peer to a worker.
type workerHandle struct {
	msgCh chan Msg

	// barrier is used to block new messages on the new worker until all old messages on the old worker are applied.
	barrier *sync.WaitGroup
	closed  bool
}

type applyBatch struct {
	msgs     []Msg
	peers    map[uint64]*peerState
	barriers []*sync.WaitGroup
}

// peerRouter routes a message to a peer.
type peerRouter struct {
	peers         sync.Map
	workerSenders []chan Msg
	storeSender   chan<- Msg
	storeFsm      *storeFsm
}

func newPeerRouter(workerSize int, storeSender chan<- Msg, storeFsm *storeFsm) *peerRouter {
	pm := &peerRouter{
		workerSenders: make([]chan Msg, workerSize),
		storeSender:   storeSender,
		storeFsm:      storeFsm,
	}
	for i := 0; i < workerSize; i++ {
		pm.workerSenders[i] = make(chan Msg, 4096)
	}
	return pm
}

func (pr *peerRouter) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *peerRouter) register(peer *peerFsm) {
	id := peer.peer.regionId
	idx := int(id) % len(pr.workerSenders)
	_, apply := newApplyFsmFromPeer(peer)
	handle := &workerHandle{
		msgCh: pr.workerSenders[idx],
	}
	newPeer := &peerState{
		handle: unsafe.Pointer(handle),
		peer:   peer,
		apply:  apply,
	}
	pr.peers.Store(id, newPeer)
}

func (pr *peerRouter) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		ps.close()
		pr.peers.Delete(regionID)
	}
}

func (pr *peerRouter) send(regionID uint64, msg Msg) error {
	p := pr.get(regionID)
	if p == nil {
		return errMailboxNotFound
	}
	return p.send(msg)
}

func (pr *peerRouter) sendStore(msg Msg) {
	pr.storeSender <- msg
}

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *peerRouter

	raftCh           chan Msg
	raftCtx          *PollContext
	raftStartTime    time.Time
	pendingProposals []*regionProposal

	applyCh  chan *applyBatch
	applyCtx *applyContext

	msgCnt            uint64
	movePeerCandidate uint64
	closeCh           <-chan struct{}
}

func newRaftWorker(b *raftPollerBuilder, ch chan Msg, pm *peerRouter) *raftWorker {
	pollCtx := b.build()
	// Make one apply router for each apply context to collect apply Msgs.
	pollCtx.applyMsgs = &applyMsgs{}
	return &raftWorker{
		raftCh:   ch,
		raftCtx:  pollCtx,
		pr:       pm,
		applyCh:  make(chan *applyBatch, 1),
		applyCtx: newApplyContext("", b.coprocessorHost, b.regionScheduler, b.engines, ch, b.cfg),
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	go rw.runApply(wg)
	var msgs []Msg
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
		atomic.AddUint64(&rw.msgCnt, uint64(len(msgs)))
		peerStateMap := make(map[uint64]*peerState)
		rw.raftCtx.pendingCount = 0
		rw.raftCtx.hasReady = false
		rw.raftStartTime = time.Now()
		batch := &applyBatch{
			peers: peerStateMap,
		}
		for _, msg := range msgs {
			if msg.Type == MsgTypeBarrier {
				batch.barriers = append(batch.barriers, msg.Data.(*sync.WaitGroup))
				continue
			}
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			delegate := &peerFsmDelegate{peerFsm: peerState.peer, ctx: rw.raftCtx}
			delegate.handleMsgs([]Msg{msg})
		}
		var movePeer uint64
		for id, peerState := range peerStateMap {
			movePeer = id
			delegate := &peerFsmDelegate{peerFsm: peerState.peer, ctx: rw.raftCtx}
			rw.pendingProposals = delegate.collectReady(rw.pendingProposals)
		}
		// Pick one peer as the candidate to be moved to other workers.
		atomic.StoreUint64(&rw.movePeerCandidate, movePeer)
		if rw.raftCtx.hasReady {
			rw.handleRaftReady(peerStateMap)
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
		peersMap[regionID] = peer
	}
	return peer
}

func (rw *raftWorker) handleRaftReady(peers map[uint64]*peerState) {
	if len(rw.pendingProposals) > 0 {
		for _, proposal := range rw.pendingProposals {
			msg := Msg{Type: MsgTypeApplyProposal, Data: proposal}
			rw.raftCtx.applyMsgs.appendMsg(proposal.RegionId, msg)
		}
		rw.pendingProposals = nil
	}
	kvWB := rw.raftCtx.kvWB
	if len(kvWB.entries) > 0 {
		err := kvWB.WriteToKV(rw.raftCtx.engine.kv)
		if err != nil {
			panic(err)
		}
		kvWB.Reset()
	}
	raftWB := rw.raftCtx.raftWB
	if len(raftWB.entries) > 0 {
		err := raftWB.WriteToRaft(rw.raftCtx.engine.raft)
		if err != nil {
			panic(err)
		}
		raftWB.Reset()
	}
	readyRes := rw.raftCtx.ReadyRes
	rw.raftCtx.ReadyRes = nil
	if len(readyRes) > 0 {
		for _, pair := range readyRes {
			regionID := pair.IC.RegionID
			newPeerFsmDelegate(peers[regionID].peer, rw.raftCtx).postRaftReadyAppend(&pair.Ready, pair.IC)
		}
	}
	dur := time.Since(rw.raftStartTime)
	if !rw.raftCtx.isBusy {
		electionTimeout := rw.raftCtx.cfg.RaftBaseTickInterval * time.Duration(rw.raftCtx.cfg.RaftElectionTimeoutTicks)
		if dur > electionTimeout {
			rw.raftCtx.isBusy = true
		}
	}
}

func (rw *raftWorker) removeQueuedSnapshots() {
	if len(rw.raftCtx.queuedSnaps) > 0 {
		rw.raftCtx.storeMetaLock.Lock()
		meta := rw.raftCtx.storeMeta
		retained := meta.pendingSnapshotRegions[:0]
		for _, region := range meta.pendingSnapshotRegions {
			if _, ok := rw.raftCtx.queuedSnaps[region.Id]; !ok {
				retained = append(retained, region)
			}
		}
		meta.pendingSnapshotRegions = retained
		rw.raftCtx.storeMetaLock.Unlock()
		rw.raftCtx.queuedSnaps = map[uint64]struct{}{}
	}
}

// runApply runs apply tasks, since it is already batched by raftCh, we don't need to batch it here.
func (rw *raftWorker) runApply(wg *sync.WaitGroup) {
	for {
		batch := <-rw.applyCh
		if batch == nil {
			wg.Done()
			return
		}
		for _, msg := range batch.msgs {
			ps := batch.peers[msg.RegionID]
			if ps == nil {
				ps = rw.pr.get(msg.RegionID)
				batch.peers[msg.RegionID] = ps
			}
			ps.apply.handleTask(rw.applyCtx, msg)
		}
		rw.applyCtx.flush()
		for _, barrier := range batch.barriers {
			barrier.Done()
		}
	}
}

// storeWorker runs store commands.
type storeWorker struct {
	store *storeFsmDelegate
}

func (sw *storeWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	for {
		var msg Msg
		select {
		case <-closeCh:
			wg.Done()
			return
		case msg = <-sw.store.receiver:
		}
		sw.store.handleMessages([]Msg{msg})
	}
}

type balancer struct {
	workers []*raftWorker
	router  *peerRouter
}

const (
	minBalanceMsgCntPerSecond = 1000
	balanceInterval           = time.Second * 10
	minBalanceMsgCnt          = minBalanceMsgCntPerSecond * uint64(balanceInterval/time.Second)
	minBalanceFactor          = 2
)

func (wb *balancer) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	ticker := time.NewTicker(balanceInterval)
	deltas := make([]uint64, len(wb.workers))
	lastCnt := make([]uint64, len(wb.workers))
	lastMove := uint64(0)
	for {
		select {
		case <-closeCh:
			wg.Done()
			return
		case <-ticker.C:
		}
		maxDelta := uint64(0)
		minDelta := uint64(math.MaxUint64)
		var maxWorker, minWorker *raftWorker
		for i := range wb.workers {
			worker := wb.workers[i]
			msgCnt := atomic.LoadUint64(&worker.msgCnt)
			delta := msgCnt - lastCnt[i]
			if delta > maxDelta {
				maxWorker = worker
			}
			if delta < minDelta {
				minWorker = worker
			}
			deltas[i] = delta
			lastCnt[i] = msgCnt
		}
		if maxDelta > minDelta*minBalanceFactor && maxDelta > minBalanceMsgCnt {
			movePeerID := atomic.LoadUint64(&maxWorker.movePeerCandidate)
			if movePeerID == lastMove {
				// Avoid to move the same peer back and force.
				continue
			}
			lastMove = movePeerID
			movePeer := wb.router.get(movePeerID)
			log.Infof("balance peer %d from busy worker to idle worker", movePeerID)
			movePeer.changeWorker(minWorker.raftCh)
		}
	}
}
