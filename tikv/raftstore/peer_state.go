package raftstore

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
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
	mu            sync.RWMutex
	peers         map[uint64]*peerState
	workerSenders []chan Msg
	storeSender   chan<- Msg
	storeFsm      *storeFsm
}

func newPeerRouter(workerSize int, storeSender chan<- Msg, storeFsm *storeFsm) *peerRouter {
	pm := &peerRouter{
		peers:         map[uint64]*peerState{},
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
	pr.mu.RLock()
	p := pr.peers[regionID]
	pr.mu.RUnlock()
	return p
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
	pr.mu.Lock()
	pr.peers[id] = newPeer
	pr.mu.Unlock()
}

func (pr *peerRouter) close(regionID uint64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	ps := pr.peers[regionID]
	if ps != nil {
		ps.close()
		delete(pr.peers, regionID)
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

	raftCh           <-chan Msg
	raftCtx          *PollContext
	raftStartTime    time.Time
	pendingProposals []*regionProposal

	applyCh  chan *applyBatch
	applyCtx *applyContext
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

// runRaft runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) runRaft() {
	var msgs []Msg
	for {
		msgs = msgs[:0]
		msgs = append(msgs, <-rw.raftCh)
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
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
		for _, peerState := range peerStateMap {
			delegate := &peerFsmDelegate{peerFsm: peerState.peer, ctx: rw.raftCtx}
			rw.pendingProposals = delegate.collectReady(rw.pendingProposals)
		}
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
func (rw *raftWorker) runApply() {
	for {
		batch := <-rw.applyCh
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

func (sw *storeWorker) run() {
	for {
		msg := <-sw.store.receiver
		sw.store.handleMessages([]Msg{msg})
	}
}
