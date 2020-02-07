package runner

import (
	"github.com/coocood/badger"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
)

type RaftLogGCTask struct {
	RaftEngine *badger.DB
	RegionID   uint64
	StartIdx   uint64
	EndIdx     uint64
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

func NewRaftLogGCTaskHandler() *raftLogGCTaskHandler {
	return &raftLogGCTaskHandler{}
}

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCTaskHandler) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {

	// Find the raft log idx range needed to be gc.
	firstIdx := startIdx
	if firstIdx == 0 {
		firstIdx = endIdx
		err := raftDb.View(func(txn *badger.Txn) error {
			startKey := meta.RaftLogKey(regionId, 0)
			ite := txn.NewIterator(badger.DefaultIteratorOptions)
			defer ite.Close()
			if ite.Seek(startKey); ite.Valid() {
				var err error
				if firstIdx, err = meta.RaftLogIndex(ite.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	if firstIdx >= endIdx {
		log.Infof("no need to gc, [regionId: %d]", regionId)
		return 0, nil
	}

	raftWb := engine_util.WriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		key := meta.RaftLogKey(regionId, idx)
		raftWb.Delete(key)
	}
	// todo, disable WAL here.
	if raftWb.Len() != 0 {
		if err := raftWb.WriteToDB(raftDb); err != nil {
			return 0, err
		}
	}
	return endIdx - firstIdx, nil
}

func (r *raftLogGCTaskHandler) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCTaskHandler) Handle(t worker.Task) {
	logGcTask := t.Data.(*RaftLogGCTask)
	log.Debugf("execute gc log. [regionId: %d, endIndex: %d]", logGcTask.RegionID, logGcTask.EndIdx)
	collected, err := r.gcRaftLog(logGcTask.RaftEngine, logGcTask.RegionID, logGcTask.StartIdx, logGcTask.EndIdx)
	if err != nil {
		log.Errorf("failed to gc. [regionId: %d, collected: %d, err: %v]", logGcTask.RegionID, collected, err)
	} else {
		log.Debugf("collected log entries. [regionId: %d, entryCount: %d]", logGcTask.RegionID, collected)
	}
	r.reportCollected(collected)
}
