package raftstore

import (
	"github.com/ngaut/log"
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

// step should be called when peerFsmDelegate received tick message.
func (t *ticker) step() {
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
				mb := r.router.mailbox(regionID)
				if mb == nil {
					log.Errorf("failed to get for %v", regionID)
					delete(r.regions, regionID)
				} else {
					mb.send(Msg{Type: MsgTypeTick}, r.router.normalScheduler)
				}
			}
		case regionID := <-r.newRegionCh:
			r.regions[regionID] = struct{}{}
		}
	}
}
