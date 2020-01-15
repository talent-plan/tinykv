package raftstore

import (
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
)

type ticker struct {
	regionID  uint64
	tick      int64
	schedules []tickSchedule
}

type tickSchedule struct {
	runAt    int64
	interval int64
}

func newTicker(regionID uint64, cfg *config.Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		regionID:  regionID,
		schedules: make([]tickSchedule, 6),
	}
	t.schedules[int(PeerTickRaft)].interval = 1
	t.schedules[int(PeerTickRaftLogGC)].interval = int64(cfg.RaftLogGCTickInterval / baseInterval)
	t.schedules[int(PeerTickSplitRegionCheck)].interval = int64(cfg.SplitRegionCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickPdHeartbeat)].interval = int64(cfg.PdHeartbeatTickInterval / baseInterval)
	t.schedules[int(PeerTickPeerStaleState)].interval = int64(cfg.PeerStaleStateCheckInterval / baseInterval)
	return t
}

func newStoreTicker(cfg *config.Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		schedules: make([]tickSchedule, 4),
	}
	t.schedules[int(StoreTickPdStoreHeartbeat)].interval = int64(cfg.PdStoreHeartbeatTickInterval / baseInterval)
	t.schedules[int(StoreTickSnapGC)].interval = int64(cfg.SnapMgrGcTickInterval / baseInterval)
	return t
}

// tickClock should be called when peerMsgHandler received tick message.
func (t *ticker) tickClock() {
	t.tick++
}

// schedule arrange the next run for the PeerTick.
func (t *ticker) schedule(tp PeerTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}

// isOnTick checks if the PeerTick should run.
func (t *ticker) isOnTick(tp PeerTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) isOnStoreTick(tp StoreTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) scheduleStore(tp StoreTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}

type tickDriver struct {
	baseTickInterval time.Duration
	newRegionCh      chan uint64
	regions          map[uint64]struct{}
	router           *router
	storeTicker      *ticker
}

func newTickDriver(baseTickInterval time.Duration, router *router, storeTicker *ticker) *tickDriver {
	return &tickDriver{
		baseTickInterval: baseTickInterval,
		newRegionCh:      make(chan uint64),
		regions:          make(map[uint64]struct{}),
		router:           router,
		storeTicker:      storeTicker,
	}
}

func (r *tickDriver) run(closeCh chan struct{}, wg *sync.WaitGroup) {
	timer := time.Tick(r.baseTickInterval)
	for {
		select {
		case <-closeCh:
			wg.Done()
			return
		case <-timer:
			for regionID, _ := range r.regions {
				if r.router.send(regionID, NewPeerMsg(MsgTypeTick, regionID, nil)) != nil {
					delete(r.regions, regionID)
				}
			}
			r.tickStore()
		case regionID := <-r.newRegionCh:
			r.regions[regionID] = struct{}{}
		}
	}
}

func (r *tickDriver) tickStore() {
	r.storeTicker.tickClock()
	for i := range r.storeTicker.schedules {
		if r.storeTicker.isOnStoreTick(StoreTick(i)) {
			r.router.sendStore(NewMsg(MsgTypeStoreTick, StoreTick(i)))
		}
	}
}
