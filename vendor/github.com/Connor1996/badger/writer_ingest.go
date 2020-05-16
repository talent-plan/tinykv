package badger

import (
	"sync"

	"github.com/Connor1996/badger/epoch"
	"github.com/Connor1996/badger/protos"
	"github.com/Connor1996/badger/table"
	"github.com/Connor1996/badger/y"
)

type ingestTask struct {
	sync.WaitGroup
	tbls []*table.Table
	cnt  int
	err  error
}

func (w *writeWorker) ingestTables(task *ingestTask) {
	ts, wg, err := w.prepareIngestTask(task)
	if err != nil {
		task.err = err
		task.Done()
		return
	}

	// Because there is no concurrent write into ingesting key ranges,
	// we can resume other writes and finish the ingest job in background.
	go func() {
		defer task.Done()
		defer w.orc.doneCommit(ts)

		ends := make([][]byte, 0, len(task.tbls))

		for _, t := range task.tbls {
			if task.err = t.SetGlobalTs(ts); task.err != nil {
				return
			}
			ends = append(ends, t.Biggest())
		}

		if wg != nil {
			wg.Wait()
		}

		for i, tbl := range task.tbls {
			if task.err = w.ingestTable(tbl, ends[i+1:]); task.err != nil {
				return
			}
			task.cnt++
		}
	}()
}

func (w *writeWorker) prepareIngestTask(task *ingestTask) (ts uint64, wg *sync.WaitGroup, err error) {
	w.orc.writeLock.Lock()
	ts = w.orc.allocTs()
	reqs := w.pollWriteCh(make([]*request, len(w.writeCh)))
	w.orc.writeLock.Unlock()

	if err = w.writeVLog(reqs); err != nil {
		return 0, nil, err
	}

	guard := w.resourceMgr.Acquire()
	defer guard.Done()
	it := w.mt.NewIterator(false)
	for _, t := range task.tbls {
		it.Seek(t.Smallest())
		if it.Valid() && y.CompareKeysWithVer(it.Key(), y.KeyWithTs(t.Biggest(), 0)) <= 0 {
			if wg, err = w.flushMemTable(); err != nil {
				return
			}
			break
		}
	}
	return
}

func (w *writeWorker) ingestTable(tbl *table.Table, splitHints [][]byte) error {
	cs := &w.lc.cstatus
	kr := keyRange{
		left:  tbl.Smallest(),
		right: tbl.Biggest(),
	}
	ref := w.resourceMgr.Acquire()
	defer ref.Done()

	var (
		targetLevel       int
		overlappingTables []*table.Table
	)

	cs.Lock()
	for targetLevel = 0; targetLevel < w.opt.TableBuilderOptions.MaxLevels; targetLevel++ {
		tbls, overlap, ok := w.checkRangeInLevel(kr, targetLevel)
		if !ok {
			// cannot place table in current level, back to previous level.
			if targetLevel != 0 {
				targetLevel--
			}
			break
		}

		overlappingTables = tbls
		if overlap {
			break
		}
	}

	if len(overlappingTables) != 0 {
		overlapLeft := overlappingTables[0].Smallest()
		if y.CompareKeysWithVer(overlapLeft, kr.left) < 0 {
			kr.left = overlapLeft
		}
		overRight := overlappingTables[len(overlappingTables)-1].Biggest()
		if y.CompareKeysWithVer(overRight, kr.right) > 0 {
			kr.right = overRight
		}
	}
	l := cs.levels[targetLevel]
	l.ranges = append(l.ranges, kr)
	cs.Unlock()
	defer l.remove(kr)

	if targetLevel != 0 && len(overlappingTables) != 0 {
		return w.runIngestCompact(targetLevel, tbl, overlappingTables, splitHints, ref)
	}

	// TODO: compression type for external table
	change := newCreateChange(tbl.ID(), targetLevel, w.opt.TableBuilderOptions.Compression)
	if err := w.manifest.addChanges([]*protos.ManifestChange{change}, nil); err != nil {
		return err
	}
	w.lc.levels[targetLevel].addTable(tbl)
	return nil
}

func (w *writeWorker) runIngestCompact(level int, tbl *table.Table, overlappingTables []*table.Table, splitHints [][]byte, guard *epoch.Guard) error {
	cd := compactDef{
		nextLevel: w.lc.levels[level],
		top:       []*table.Table{tbl},
		nextRange: getKeyRange(overlappingTables),
	}
	w.lc.fillBottomTables(&cd, overlappingTables)
	newTables, err := w.lc.compactBuildTables(level-1, cd, w.limiter, splitHints)
	if err != nil {
		return err
	}

	var changes []*protos.ManifestChange
	for _, t := range newTables {
		changes = append(changes, newCreateChange(t.ID(), level, w.opt.TableBuilderOptions.Compression))
	}
	for _, t := range cd.bot {
		changes = append(changes, newDeleteChange(t.ID()))
	}

	if err := w.manifest.addChanges(changes, nil); err != nil {
		return err
	}
	cd.nextLevel.replaceTables(newTables, &cd, guard)
	return nil
}

func (w *writeWorker) overlapWithFlushingMemTables(kr keyRange) bool {
	for _, mt := range w.imm {
		it := mt.NewIterator(false)
		it.Seek(kr.left)
		if !it.Valid() || y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
			return true
		}
	}
	return false
}

func (w *writeWorker) checkRangeInLevel(kr keyRange, level int) (overlappingTables []*table.Table, overlap bool, ok bool) {
	cs := &w.lc.cstatus
	handler := w.lc.levels[level]
	handler.RLock()
	defer handler.RUnlock()

	if len(handler.tables) == 0 && level != 0 {
		return nil, false, false
	}

	l := cs.levels[level]
	if l.overlapsWith(kr) {
		return nil, false, false
	}

	var left, right int
	if level == 0 {
		left, right = 0, len(handler.tables)
	} else {
		left, right = handler.overlappingTables(levelHandlerRLocked{}, kr)
	}

	for i := left; i < right; i++ {
		it := handler.tables[i].NewIterator(false)
		it.Seek(kr.left)
		if it.Valid() && y.CompareKeysWithVer(it.Key(), kr.right) <= 0 {
			overlap = true
			break
		}
	}
	return handler.tables[left:right], overlap, true
}
