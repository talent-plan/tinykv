/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Connor1996/badger/epoch"
	"github.com/Connor1996/badger/protos"
	"github.com/Connor1996/badger/skl"
	"github.com/Connor1996/badger/table"
	"github.com/Connor1996/badger/y"
	"github.com/dgraph-io/ristretto"
	farm "github.com/dgryski/go-farm"
	"github.com/ncw/directio"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"golang.org/x/time/rate"
)

var (
	txnKey = []byte("!badger!txn") // For indicating end of entries in txn.
)

type closers struct {
	updateSize *y.Closer
	compactors *y.Closer
	memtable   *y.Closer
	writes     *y.Closer
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers   closers
	mt        *table.MemTable   // Our latest (actively written) in-memory table
	imm       []*table.MemTable // Add here only AFTER pushing to flushChan.
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	logOff    logOffset // less than or equal to a pointer to the last vlog value put into mt
	syncedFid uint32    // The log fid that has been flushed to SST, older log files are safe to be deleted.
	writeCh   chan *request
	flushChan chan *flushTask // For flushing memtables.
	ingestCh  chan *ingestTask

	// mem table buffer to avoid expensive allocating big chunk of memory
	memTableCh chan *table.MemTable

	orc              *oracle
	minReadTsTracker safeTsTracker

	limiter *rate.Limiter

	blockCache *ristretto.Cache

	metrics  *y.MetricsSet
	lsmSize  int64
	vlogSize int64

	blobManger blobManager

	resourceMgr *epoch.ResourceManager
}

const (
	kvWriteChCapacity = 1000
)

func replayFunction(out *DB) func(Entry) error {
	type txnEntry struct {
		nk []byte
		v  y.ValueStruct
	}

	var txn []txnEntry
	var lastCommit uint64

	toLSM := func(nk []byte, vs y.ValueStruct) {
		e := table.Entry{Key: nk, Value: vs}
		out.ensureRoomForWrite(e.EstimateSize())
		out.mt.PutToSkl(nk, vs)
	}

	first := true
	return func(e Entry) error { // Function for replaying.
		if first {
			log.Infof("First key=%s\n", e.Key)
		}
		first = false

		if out.orc.curRead < y.ParseTs(e.Key) {
			out.orc.curRead = y.ParseTs(e.Key)
		}

		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		nv := make([]byte, len(e.Value))
		copy(nv, e.Value)

		v := y.ValueStruct{
			Value:    nv,
			Meta:     e.meta,
			UserMeta: e.UserMeta,
		}

		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				return errors.Wrapf(err, "Unable to parse txn fin: %q", e.Value)
			}
			y.Assert(lastCommit == txnTs)
			y.Assert(len(txn) > 0)
			// Got the end of txn. Now we can store them.
			for _, t := range txn {
				toLSM(t.nk, t.v)
			}
			txn = txn[:0]
			lastCommit = 0

		} else if e.meta&bitTxn == 0 {
			// This entry is from a rewrite.
			toLSM(nk, v)

			// We shouldn't get this entry in the middle of a transaction.
			y.Assert(lastCommit == 0)
			y.Assert(len(txn) == 0)

		} else {
			txnTs := y.ParseTs(nk)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			y.Assert(lastCommit == txnTs)
			te := txnEntry{nk: nk, v: v}
			txn = append(txn, te)
		}
		return nil
	}
}

// Open returns a new DB object.
func Open(opt Options) (db *DB, err error) {
	log.SetLevelByString("warning")
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	if opt.ValueThreshold > math.MaxUint16-16 {
		return nil, ErrValueThreshold
	}

	if opt.ReadOnly {
		// Can't truncate if the DB is read only.
		opt.Truncate = false
	}

	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return nil, y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return nil, y.Wrapf(err, "Cannot find Dir for read-only open: %q", path)
			}
			// Try to create the directory
			err = os.Mkdir(path, 0700)
			if err != nil {
				return nil, y.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}
	absDir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	absValueDir, err := filepath.Abs(opt.ValueDir)
	if err != nil {
		return nil, err
	}
	var dirLockGuard, valueDirLockGuard *directoryLockGuard
	dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.release()
		}
	}()
	if absValueDir != absDir {
		valueDirLockGuard, err = acquireDirectoryLock(opt.ValueDir, lockFile, opt.ReadOnly)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if valueDirLockGuard != nil {
			_ = valueDirLockGuard.release()
		}
	}()
	if !(opt.ValueLogFileSize <= 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}
	manifestFile, manifest, err := openOrCreateManifestFile(opt.Dir, opt.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	orc := &oracle{
		isManaged:  opt.managedTxns,
		nextCommit: 1,
		commits:    make(map[uint64]uint64),
	}

	config := ristretto.Config{
		// Use 5% of cache memory for storing counters.
		NumCounters: int64(float64(opt.MaxCacheSize) * 0.05 * 2),
		MaxCost:     int64(float64(opt.MaxCacheSize) * 0.95),
		BufferItems: 64,
		// Enable metrics once https://github.com/dgraph-io/ristretto/issues/92 is resolved.
		Metrics: false,
	}
	cache, err := ristretto.NewCache(&config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cache")
	}
	db = &DB{
		imm:           make([]*table.MemTable, 0, opt.NumMemtables),
		flushChan:     make(chan *flushTask, opt.NumMemtables),
		writeCh:       make(chan *request, kvWriteChCapacity),
		memTableCh:    make(chan *table.MemTable, 1),
		ingestCh:      make(chan *ingestTask),
		opt:           opt,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		orc:           orc,
		metrics:       y.NewMetricSet(opt.Dir),
		blockCache:    cache,
	}
	db.vlog.metrics = db.metrics

	rateLimit := opt.TableBuilderOptions.BytesPerSecond
	if rateLimit > 0 {
		db.limiter = rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
	}

	db.closers.memtable = y.NewCloser(1)
	go func() {
		lc := db.closers.memtable
		for {
			select {
			case db.memTableCh <- table.NewMemTable(arenaSize(db.opt)):
			case <-lc.HasBeenClosed():
				lc.Done()
				return
			}
		}
	}()

	// Calculate initial size.
	db.calculateSize()
	db.closers.updateSize = y.NewCloser(1)
	go db.updateSize(db.closers.updateSize)
	db.mt = <-db.memTableCh

	db.resourceMgr = epoch.NewResourceManager(&db.minReadTsTracker)

	// newLevelsController potentially loads files in directory.
	if db.lc, err = newLevelsController(db, &manifest, db.resourceMgr, opt.TableBuilderOptions); err != nil {
		return nil, err
	}
	if err = db.blobManger.Open(db, opt); err != nil {
		return nil, err
	}

	if !opt.ReadOnly {
		db.closers.compactors = y.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)

		db.closers.memtable.AddRunning(1)
		go db.runFlushMemTable(db.closers.memtable) // Need levels controller to be up.
	}

	if err = db.vlog.Open(db, opt); err != nil {
		return nil, err
	}

	var logOff logOffset
	head := manifest.Head
	if head != nil {
		db.orc.curRead = head.Version
		logOff.fid = head.LogID
		logOff.offset = head.LogOffset
	}

	// lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
	// written value log entry that we replay.  (Subsequent value log entries might be _less_
	// than lastUsedCasCounter, if there was value log gc so we have to max() values while
	// replaying.)
	// out.lastUsedCasCounter = item.casCounter
	// TODO: Figure this out. This would update the read timestamp, and set nextCommitTs.

	replayCloser := startWriteWorker(db)

	if err = db.vlog.Replay(logOff, replayFunction(db)); err != nil {
		return db, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.
	// Now that we have the curRead, we can update the nextCommit.
	db.orc.Lock()
	db.orc.nextCommit = db.orc.curRead + 1
	db.orc.Unlock()

	db.writeCh = make(chan *request, kvWriteChCapacity)
	db.closers.writes = startWriteWorker(db)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// DeleteFilesInRange delete files in [start, end).
// If some file contains keys outside the range, they will not be deleted.
// This function is designed to reclaim space quickly.
// If you want to ensure no future transaction can read keys in range,
// considering iterate and delete the remained keys, or using compaction filter to cleanup them asynchronously.
func (db *DB) DeleteFilesInRange(start, end []byte) {
	var (
		changes   []*protos.ManifestChange
		pruneTbls []*table.Table
		startKey  = y.KeyWithTs(start, math.MaxUint64)
		endKey    = y.KeyWithTs(end, 0)
		guard     = db.resourceMgr.Acquire()
	)

	for _, lc := range db.lc.levels {
		lc.Lock()
		left, right := 0, len(lc.tables)
		if lc.level > 0 {
			left, right = getTablesInRange(lc.tables, startKey, endKey)
		}
		if left >= right {
			lc.Unlock()
			continue
		}

		newTables := lc.tables[:left]
		for _, tbl := range lc.tables[left:right] {
			if !isRangeCoversTable(startKey, endKey, tbl) || tbl.IsCompacting() {
				newTables = append(newTables, tbl)
				continue
			}
			pruneTbls = append(pruneTbls, tbl)
			changes = append(changes, newDeleteChange(tbl.ID()))
		}
		newTables = append(newTables, lc.tables[right:]...)
		for i := len(newTables); i < len(lc.tables); i++ {
			lc.tables[i] = nil
		}
		assertTablesOrder(newTables)
		lc.tables = newTables
		lc.Unlock()
	}

	db.manifest.addChanges(changes, nil)
	var discardStats DiscardStats
	deletes := make([]epoch.Resource, len(pruneTbls))
	for i, tbl := range pruneTbls {
		it := tbl.NewIterator(false)
		// TODO: use rate limiter to avoid burst IO.
		for it.Rewind(); it.Valid(); it.Next() {
			discardStats.collect(it.Value())
		}
		deletes[i] = tbl
	}
	if len(discardStats.ptrs) > 0 {
		db.blobManger.discardCh <- &discardStats
	}
	guard.Delete(deletes)
	guard.Done()
}

func isRangeCoversTable(start, end []byte, t *table.Table) bool {
	left := y.CompareKeysWithVer(start, t.Smallest()) <= 0
	right := y.CompareKeysWithVer(t.Biggest(), end) < 0
	return left && right
}

// NewExternalTableBuilder returns a new sst builder.
func (db *DB) NewExternalTableBuilder(f *os.File, limiter *rate.Limiter) *table.Builder {
	return table.NewExternalTableBuilder(f, limiter, db.opt.TableBuilderOptions)
}

// ErrExternalTableOverlap returned by IngestExternalFiles when files overlaps.
var ErrExternalTableOverlap = errors.New("keys of external tables has overlap")

// IngestExternalFiles ingest external constructed tables into DB.
// Note: insure there is no concurrent write overlap with tables to be ingested.
func (db *DB) IngestExternalFiles(files []*os.File) (int, error) {
	tbls, err := db.prepareExternalFiles(files)
	if err != nil {
		return 0, err
	}

	if err := db.checkExternalTables(tbls); err != nil {
		return 0, err
	}

	task := &ingestTask{tbls: tbls}
	task.Add(1)
	db.ingestCh <- task
	task.Wait()
	return task.cnt, task.err
}

func (db *DB) prepareExternalFiles(files []*os.File) ([]*table.Table, error) {
	tbls := make([]*table.Table, len(files))
	for i, fd := range files {
		id := db.lc.reserveFileID()
		filename := table.NewFilename(id, db.opt.Dir)

		err := os.Link(fd.Name(), filename)
		if err != nil {
			return nil, err
		}

		fd, err := os.OpenFile(filename, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}

		tbl, err := table.OpenTable(fd, db.opt.TableLoadingMode, db.opt.TableBuilderOptions.Compression, db.blockCache)
		if err != nil {
			return nil, err
		}

		tbls[i] = tbl
	}

	sort.Slice(tbls, func(i, j int) bool {
		return bytes.Compare(tbls[i].Smallest(), tbls[j].Smallest()) < 0
	})

	return tbls, syncDir(db.lc.kv.opt.Dir)
}

func (db *DB) checkExternalTables(tbls []*table.Table) error {
	keys := make([][]byte, 0, len(tbls)*2)
	for _, t := range tbls {
		keys = append(keys, t.Smallest(), t.Biggest())
	}
	ok := sort.SliceIsSorted(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	if !ok {
		return ErrExternalTableOverlap
	}

	for i := 1; i < len(keys)-1; i += 2 {
		if bytes.Compare(keys[i], keys[i+1]) == 0 {
			return ErrExternalTableOverlap
		}
	}

	return nil
}

// CacheMetrics returns the metrics for the underlying cache.
func (db *DB) CacheMetrics() *ristretto.Metrics {
	// Do not enable ristretto metrics in badger until issue
	// https://github.com/dgraph-io/ristretto/issues/92 is resolved.
	// return db.blockCache.Metrics()
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates
// make their way to disk. Calling DB.Close() multiple times is not safe and would
// cause panic.
func (db *DB) Close() (err error) {
	log.Infof("Closing database")

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Now close the value log.
	if vlogErr := db.vlog.Close(); err == nil {
		err = errors.Wrap(vlogErr, "DB.Close")
	}

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if !db.mt.Empty() {
		log.Infof("Flushing memtable")
		for {
			pushedFlushTask := func() bool {
				db.Lock()
				defer db.Unlock()
				y.Assert(db.mt != nil)
				select {
				case db.flushChan <- newFlushTask(db.mt, db.logOff):
					db.imm = append(db.imm, db.mt) // Flusher will attempt to remove this from s.imm.
					db.mt = nil                    // Will segfault if we try writing!
					log.Infof("pushed to flush chan\n")
					return true
				default:
					// If we fail to push, we need to unlock and wait for a short while.
					// The flushing operation needs to update s.imm. Otherwise, we have a deadlock.
					// TODO: Think about how to do this more cleanly, maybe without any locks.
				}
				return false
			}()
			if pushedFlushTask {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	db.flushChan <- newFlushTask(nil, logOffset{}) // Tell flusher to quit.

	if db.closers.memtable != nil {
		db.closers.memtable.SignalAndWait()
		log.Infof("Memtable flushed")
	}
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
		log.Infof("Compaction finished")
	}

	// Force Compact L0
	// We don't need to care about cstatus since no parallel compaction is running.
	cd := compactDef{
		thisLevel: db.lc.levels[0],
		nextLevel: db.lc.levels[1],
	}
	guard := db.resourceMgr.Acquire()
	defer guard.Done()
	if db.lc.fillTablesL0(&cd) {
		if err := db.lc.runCompactDef(0, cd, nil, guard); err != nil {
			log.Infof("\tLOG Compact FAILED with error: %+v: %+v", err, cd)
		}
	} else {
		log.Infof("fillTables failed for level zero. No compaction required")
	}

	if lcErr := db.lc.close(); err == nil {
		err = errors.Wrap(lcErr, "DB.Close")
	}
	log.Infof("Waiting for closer")
	db.closers.updateSize.SignalAndWait()
	db.blockCache.Close()

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = errors.Wrap(manifestErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(db.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}
	if syncErr := syncDir(db.opt.ValueDir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}

	return err
}

const (
	lockFile = "LOCK"
)

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// getMemtables returns the current memtables and get references.
func (db *DB) getMemTables() []*table.MemTable {
	tables := make([]*table.MemTable, 1, 8)
	db.RLock()
	defer db.RUnlock()
	// Get mutable memtable.
	tables[0] = db.mt

	// Get immutable memtables.
	last := len(db.imm) - 1
	for i := range db.imm {
		immt := db.imm[last-i]
		tables = append(tables, immt)
	}
	return tables
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
//
// IMPORTANT: We should never write an entry with an older timestamp for the same key, We need to
// maintain this invariant to search for the latest value of a key, or else we need to search in all
// tables and find the max version among them.  To maintain this invariant, we also need to ensure
// that all versions of a key are always present in the same table from level 1, because compaction
// can push any table down.
func (db *DB) get(key []byte) y.ValueStruct {
	tables := db.getMemTables() // Lock should be released.

	db.metrics.NumGets.Inc()
	for _, table := range tables {
		vs := table.Get(key)
		db.metrics.NumMemtableGets.Inc()
		if vs.Valid() {
			return vs
		}
	}
	keyHash := farm.Fingerprint64(y.ParseKey(key))
	return db.lc.get(key, keyHash)
}

func (db *DB) multiGet(pairs []keyValuePair) {
	tables := db.getMemTables() // Lock should be released.

	var foundCount, mtGets int
	for _, table := range tables {
		for j := range pairs {
			pair := &pairs[j]
			if pair.found {
				continue
			}
			val := table.Get(pair.key)
			if val.Valid() {
				pair.val = val
				pair.found = true
				foundCount++
			}
			mtGets++
		}
	}
	db.metrics.NumMemtableGets.Add(float64(mtGets))
	db.metrics.NumGets.Add(float64(len(pairs)))

	if foundCount == len(pairs) {
		return
	}
	db.lc.multiGet(pairs)
}

func (db *DB) updateOffset(off logOffset) {
	db.Lock()
	y.Assert(!off.Less(db.logOff))
	db.logOff = off
	db.Unlock()
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) sendToWriteCh(entries []*Entry) (*request, error) {
	var count, size int64
	for _, e := range entries {
		size += int64(e.estimateSize())
		count++
	}

	// We can only service one request because we need each txn to be stored in a contigous section.
	// Txns should not interleave among other txns or rewrites.
	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	db.writeCh <- req // Handled in writeWorker.
	db.metrics.NumPuts.Add(float64(len(entries)))

	return req, nil
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//   Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*Entry) error {
	sort.Slice(entries, func(i, j int) bool {
		return y.CompareKeysWithVer(entries[i].Key, entries[j].Key) < 0
	})
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//   err := kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//   }
func (db *DB) batchSetAsync(entries []*Entry, f func(error)) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

// ensureRoomForWrite is always called serially.
func (db *DB) ensureRoomForWrite(minSize int64) (int64, error) {
	free := db.opt.MaxTableSize - db.mt.MemSize()
	if free >= minSize {
		return free, nil
	}
	_, err := db.flushMemTable()
	return db.opt.MaxTableSize, err
}

func (db *DB) flushMemTable() (*sync.WaitGroup, error) {
	newMemTable := <-db.memTableCh
	for {
		db.Lock()
		ft := newFlushTask(db.mt, db.logOff)
		select {
		case db.flushChan <- ft:
			log.Infof("Flushing memtable, mt.size=%d, size of flushChan: %d\n",
				db.mt.MemSize(), len(db.flushChan))
			// We manage to push this task. Let's modify imm.
			db.imm = append(db.imm, db.mt)
			db.mt = newMemTable
			db.Unlock()
			// New memtable is empty. We certainly have room.
			return &ft.wg, nil
		default:
			db.Unlock()
			// log.Warnf("Making room for writes")
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func arenaSize(opt Options) int64 {
	return opt.MaxTableSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func (db *DB) writeLevel0Table(s *table.MemTable, f *os.File) error {
	iter := s.NewIterator(false)
	var (
		bb                   *blobFileBuilder
		numWrite, bytesWrite int
		err                  error
	)
	b := table.NewTableBuilder(f, db.limiter, 0, db.opt.TableBuilderOptions)
	defer b.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if db.opt.ValueThreshold > 0 && len(value.Value) > db.opt.ValueThreshold {
			if bb == nil {
				if bb, err = db.newBlobFileBuilder(); err != nil {
					return y.Wrap(err)
				}
			}

			bp, err := bb.append(value.Value)
			if err != nil {
				return err
			}
			value.Meta |= bitValuePointer
			value.Value = bp
		}
		if err := b.Add(key, value); err != nil {
			return err
		}
		numWrite++
		bytesWrite += len(key) + int(value.EncodedSize())
	}
	stats := &y.CompactionStats{
		KeysWrite:  numWrite,
		BytesWrite: bytesWrite,
	}
	db.lc.levels[0].metrics.UpdateCompactionStats(stats)

	if err := b.Finish(); err != nil {
		return y.Wrap(err)
	}
	if bb != nil {
		bf, err1 := bb.finish()
		if err1 != nil {
			return err1
		}
		log.Infof("build L0 blob:%d size:%d", bf.fid, bf.fileSize)
		err1 = db.blobManger.addFile(bf)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (db *DB) newBlobFileBuilder() (*blobFileBuilder, error) {
	return newBlobFileBuilder(db.blobManger.allocFileID(), db.opt.Dir, db.opt.TableBuilderOptions.WriteBufferSize)
}

type flushTask struct {
	mt  *table.MemTable
	off logOffset
	wg  sync.WaitGroup
}

func newFlushTask(mt *table.MemTable, off logOffset) *flushTask {
	ft := &flushTask{mt: mt, off: off}
	ft.wg.Add(1)
	return ft
}

// TODO: Ensure that this function doesn't return, or is handled by another wrapper function.
// Otherwise, we would have no goroutine which can flush memtables.
func (db *DB) runFlushMemTable(c *y.Closer) error {
	defer c.Done()

	for ft := range db.flushChan {
		if ft.mt == nil {
			return nil
		}
		guard := db.resourceMgr.Acquire()
		var headInfo *protos.HeadInfo
		if !ft.mt.Empty() {
			headInfo = &protos.HeadInfo{
				// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
				// commits.
				Version:   db.orc.commitTs(),
				LogID:     ft.off.fid,
				LogOffset: ft.off.offset,
			}
			// Store badger head even if vptr is zero, need it for readTs
			log.Infof("Storing offset: %+v\n", ft.off)
		}

		fileID := db.lc.reserveFileID()
		fileName := table.NewFilename(fileID, db.opt.Dir)
		fd, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return y.Wrap(err)
		}

		// Don't block just to sync the directory entry.
		dirSyncCh := make(chan error)
		go func() { dirSyncCh <- syncDir(db.opt.Dir) }()

		err = db.writeLevel0Table(ft.mt, fd)
		dirSyncErr := <-dirSyncCh
		if err != nil {
			log.Errorf("ERROR while writing to level 0: %v", err)
			return err
		}
		if dirSyncErr != nil {
			log.Errorf("ERROR while syncing level directory: %v", dirSyncErr)
			return err
		}
		atomic.StoreUint32(&db.syncedFid, ft.off.fid)
		fd.Close()
		fd, err = os.OpenFile(fileName, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		tbl, err := table.OpenTable(fd, db.opt.TableLoadingMode, db.opt.TableBuilderOptions.Compression, db.blockCache)
		if err != nil {
			log.Infof("ERROR while opening table: %v", err)
			return err
		}
		err = db.lc.addLevel0Table(tbl, headInfo)
		if err != nil {
			return err
		}

		// Update s.imm. Need a lock.
		db.Lock()
		y.Assert(ft.mt == db.imm[0]) //For now, single threaded.
		db.imm = db.imm[1:]
		guard.Delete([]epoch.Resource{ft.mt})
		db.Unlock()
		guard.Done()
		ft.wg.Done()
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// This function does a filewalk, calculates the size of vlog and sst files and stores it in
// y.LSMSize and y.VlogSize.
func (db *DB) calculateSize() {
	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if ext == ".sst" {
				lsmSize += info.Size()
			} else if ext == ".vlog" {
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			log.Infof("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	lsmSize, vlogSize := totalSize(db.opt.Dir)
	// If valueDir is different from dir, we'd have to do another walk.
	if db.opt.ValueDir != db.opt.Dir {
		_, vlogSize = totalSize(db.opt.ValueDir)
	}
	atomic.StoreInt64(&db.lsmSize, lsmSize)
	atomic.StoreInt64(&db.vlogSize, vlogSize)
	db.metrics.LSMSize.Set(float64(lsmSize))
	db.metrics.VlogSize.Set(float64(vlogSize))
}

func (db *DB) updateSize(c *y.Closer) {
	defer c.Done()

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-c.HasBeenClosed():
			return
		}
	}
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm int64, vlog int64) {
	return atomic.LoadInt64(&db.lsmSize), atomic.LoadInt64(&db.vlogSize)
}

func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}

func (db *DB) GetVLogOffset() uint64 {
	return db.vlog.getMaxPtr()
}

// IterateVLog iterates VLog for external replay, this function should be called only when there is no
// concurrent write operation on the DB.
func (db *DB) IterateVLog(offset uint64, fn func(e Entry)) error {
	startFid := uint32(offset >> 32)
	vOffset := uint32(offset)
	for fid := startFid; fid <= db.vlog.maxFid(); fid++ {
		lf, err := db.vlog.getFile(fid)
		if err != nil {
			return err
		}
		if fid != startFid {
			vOffset = 0
		}
		endOffset, err := db.vlog.iterate(lf, vOffset, func(e Entry) error {
			if e.meta&bitTxn > 0 {
				fn(e)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if fid == db.vlog.maxFid() {
			_, err = lf.fd.Seek(int64(endOffset), io.SeekStart)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) getCompactSafeTs() uint64 {
	return atomic.LoadUint64(&db.minReadTsTracker.safeTs)
}

type safeTsTracker struct {
	safeTs uint64

	maxInactive uint64
	minActive   uint64
}

func (t *safeTsTracker) Begin() {
	// t.maxInactive = 0
	t.minActive = math.MaxUint64
}

func (t *safeTsTracker) Inspect(payload interface{}, isActive bool) {
	ts, ok := payload.(uint64)
	if !ok {
		return
	}

	if isActive {
		if ts < t.minActive {
			t.minActive = ts
		}
	} else {
		if ts > t.maxInactive {
			t.maxInactive = ts
		}
	}
}

func (t *safeTsTracker) End() {
	var safe uint64
	if t.minActive == math.MaxUint64 {
		safe = t.maxInactive
	} else {
		safe = t.minActive - 1
	}

	if safe > t.safeTs {
		atomic.StoreUint64(&t.safeTs, safe)
	}
}
