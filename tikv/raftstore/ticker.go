package raftstore

import (
	"time"
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

func newTicker(regionID uint64, cfg *Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		regionID:  regionID,
		schedules: make([]tickSchedule, 6),
	}
	t.schedules[int(PeerTickRaft)].interval = 1
	t.schedules[int(PeerTickRaftLogGC)].interval = int64(cfg.RaftLogGCTickInterval / baseInterval)
	t.schedules[int(PeerTickSplitRegionCheck)].interval = int64(cfg.SplitRegionCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickPdHeartbeat)].interval = int64(cfg.PdHeartbeatTickInterval / baseInterval)
	t.schedules[int(PeerTickCheckMerge)].interval = int64(cfg.MergeCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickPeerStaleState)].interval = int64(cfg.PeerStaleStateCheckInterval / baseInterval)
	return t
}

func newStoreTicker(cfg *Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		schedules: make([]tickSchedule, 4),
	}
	t.schedules[int(StoreTickCompactCheck)].interval = int64(cfg.RegionCompactCheckInterval / baseInterval)
	t.schedules[int(StoreTickPdStoreHeartbeat)].interval = int64(cfg.PdStoreHeartbeatTickInterval / baseInterval)
	t.schedules[int(StoreTickSnapGC)].interval = int64(cfg.SnapMgrGcTickInterval / baseInterval)
	t.schedules[int(StoreTickConsistencyCheck)].interval = int64(cfg.ConsistencyCheckInterval / baseInterval)
	return t
}

// tickClock should be called when peerFsmDelegate received tick message.
func (t *ticker) tickClock() {
	t.tick++
}

// schedule arrange the next run for the PeerTick.
func (t *ticker) schedule(tp PeerTick) {
	sched := &t.schedules[int(tp)]
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
	sched.runAt = t.tick + sched.interval
}

type tickDriver struct {
	baseTickInterval time.Duration
	newRegionCh      chan uint64
	regions          map[uint64]struct{}
	router           *router
}

func (r *tickDriver) run() {
	timer := time.Tick(r.baseTickInterval)
	for {
		select {
		case <-timer:
			for regionID, _ := range r.regions {
				if r.router.send(regionID, NewPeerMsg(MsgTypeTick, regionID, nil)) != nil {
					delete(r.regions, regionID)
				}
			}
		case regionID := <-r.newRegionCh:
			r.regions[regionID] = struct{}{}
		}
	}
}
