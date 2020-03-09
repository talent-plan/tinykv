package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	raftCh chan message.Msg
	ctx    *GlobalContext

	applyCh chan []message.Msg

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh:  pm.peerSender,
		ctx:     ctx,
		applyCh: make(chan []message.Msg, 4096),
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
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			newPeerMsgHandler(peerState.peer, rw.applyCh, rw.ctx).HandleMsgs(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.applyCh, rw.ctx).HandleRaftReady()
		}
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

type applyWorker struct {
	pr      *router
	applyCh chan []message.Msg
	ctx     *GlobalContext
}

func newApplyWorker(ctx *GlobalContext, ch chan []message.Msg, pr *router) *applyWorker {
	return &applyWorker{
		pr:      pr,
		applyCh: ch,
		ctx:     ctx,
	}
}

// run runs apply tasks, since it is already batched by raftCh, we don't need to batch it here.
func (aw *applyWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		msgs := <-aw.applyCh
		if msgs == nil {
			return
		}
		for _, msg := range msgs {
			ps := aw.pr.get(msg.RegionID)
			if ps == nil {
				continue
			}
			ps.apply.handleTask(msg)
		}
	}
}
