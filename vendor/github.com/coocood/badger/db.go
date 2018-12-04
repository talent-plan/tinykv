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
	"expvar"
	"golang.org/x/time/rate"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"

	"github.com/coocood/badger/skl"
	"github.com/coocood/badger/table"
	"github.com/coocood/badger/y"
	"github.com/pkg/errors"
)

var (
	badgerPrefix = []byte("!badger!")     // Prefix for internal keys used by badger.
	head         = []byte("!badger!head") // For storing value offset for replay.
	txnKey       = []byte("!badger!txn")  // For indicating end of entries in txn.
	badgerMove   = []byte("!badger!move") // For key-value pairs which got moved during GC.
)

type closers struct {
	updateSize *y.Closer
	compactors *y.Closer
	memtable   *y.Closer
	writes     *y.Closer
	valueGC    *y.Closer
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers   closers
	mt        *skl.Skiplist   // Our latest (actively written) in-memory table
	imm       []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	vptr      valuePointer // less than or equal to a pointer to the last vlog value put into mt
	writeCh   chan *request
	flushChan chan flushTask // For flushing memtables.

	// mem table buffer to avoid expensive allocating big chunk of memory
	memTableCh chan *skl.Skiplist

	orc *oracle

	limiter *rate.Limiter
}

const (
	kvWriteChCapacity = 1000
)

func replayFunction(out *DB) func(Entry, valuePointer) error {
	type txnEntry struct {
		nk []byte
		v  y.ValueStruct
	}

	var txn []txnEntry
	var lastCommit uint64

	toLSM := func(nk []byte, vs y.ValueStruct) {
		for err := out.ensureRoomForWrite(); err != nil; err = out.ensureRoomForWrite() {
			log.Infof("Replay: Making room for writes")
			time.Sleep(10 * time.Millisecond)
		}
		out.mt.Put(nk, vs)
	}

	first := true
	return func(e Entry, vp valuePointer) error { // Function for replaying.
		if first {
			log.Infof("First key=%s\n", e.Key)
		}
		first = false

		if out.orc.curRead < y.ParseTs(e.Key) {
			out.orc.curRead = y.ParseTs(e.Key)
		}

		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.meta
		if out.shouldWriteValueToLSM(&e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = make([]byte, vptrSize)
			vp.Encode(nv)
			meta = meta | bitValuePointer
		}

		v := y.ValueStruct{
			Value:    nv,
			Meta:     meta,
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
		readMark:   y.WaterMark{},
	}
	orc.readMark.Init()

	db = &DB{
		imm:           make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan:     make(chan flushTask, opt.NumMemtables),
		writeCh:       make(chan *request, kvWriteChCapacity),
		memTableCh:    make(chan *skl.Skiplist, 1),
		opt:           opt,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		orc:           orc,
	}

	rateLimit := opt.TableBuilderOptions.BytesPerSecond
	if rateLimit > 0 {
		db.limiter = rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
	}

	go func() {
		for {
			db.memTableCh <- skl.NewSkiplist(arenaSize(db.opt))
		}
	}()

	// Calculate initial size.
	db.calculateSize()
	db.closers.updateSize = y.NewCloser(1)
	go db.updateSize(db.closers.updateSize)
	db.mt = <-db.memTableCh

	// newLevelsController potentially loads files in directory.
	if db.lc, err = newLevelsController(db, &manifest, opt.TableBuilderOptions); err != nil {
		return nil, err
	}

	if !opt.ReadOnly {
		db.closers.compactors = y.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)

		db.closers.memtable = y.NewCloser(1)
		go db.flushMemtable(db.closers.memtable) // Need levels controller to be up.
	}

	if err = db.vlog.Open(db, opt); err != nil {
		return nil, err
	}

	headKey := y.KeyWithTs(head, math.MaxUint64)
	// Need to pass with timestamp, lsm get removes the last 8 bytes and compares key
	vs, err := db.get(headKey)
	if err != nil {
		return nil, errors.Wrap(err, "Retrieving head")
	}
	db.orc.curRead = vs.Version
	var vptr valuePointer
	if len(vs.Value) > 0 {
		vptr.Decode(vs.Value)
	}

	// lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
	// written value log entry that we replay.  (Subsequent value log entries might be _less_
	// than lastUsedCasCounter, if there was value log gc so we have to max() values while
	// replaying.)
	// out.lastUsedCasCounter = item.casCounter
	// TODO: Figure this out. This would update the read timestamp, and set nextCommitTs.

	replayCloser := startWriteWorker(db)

	if err = db.vlog.Replay(vptr, replayFunction(db)); err != nil {
		return db, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.
	// Now that we have the curRead, we can update the nextCommit.
	db.orc.nextCommit = db.orc.curRead + 1

	db.writeCh = make(chan *request, kvWriteChCapacity)
	db.closers.writes = startWriteWorker(db)

	db.closers.valueGC = y.NewCloser(1)
	go db.vlog.waitOnGC(db.closers.valueGC)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates
// make their way to disk. Calling DB.Close() multiple times is not safe and would
// cause panic.
func (db *DB) Close() (err error) {
	log.Infof("Closing database")
	// Stop value GC first.
	db.closers.valueGC.SignalAndWait()

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
				case db.flushChan <- flushTask{db.mt, db.vptr}:
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
	db.flushChan <- flushTask{nil, valuePointer{}} // Tell flusher to quit.

	if db.closers.memtable != nil {
		db.closers.memtable.Wait()
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
	if db.lc.fillTablesL0(&cd) {
		if err := db.lc.runCompactDef(0, cd, nil); err != nil {
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
func (db *DB) getMemTables() []*skl.Skiplist {
	tables := make([]*skl.Skiplist, 1, 8)
	db.RLock()
	defer db.RUnlock()
	// Get mutable memtable.
	tables[0] = db.mt
	tables[0].IncrRef()

	// Get immutable memtables.
	last := len(db.imm) - 1
	for i := range db.imm {
		immt := db.imm[last-i]
		immt.IncrRef()
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
func (db *DB) get(key []byte) (y.ValueStruct, error) {
	tables := db.getMemTables() // Lock should be released.
	defer func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}()

	y.NumGets.Add(1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGets.Add(1)
		if vs.Meta != 0 || vs.Value != nil {
			return vs, nil
		}
	}
	return db.lc.get(key)
}

func (db *DB) updateOffset(ptrs []valuePointer) {
	var ptr valuePointer
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}

	db.Lock()
	y.Assert(!ptr.Less(db.vptr))
	db.vptr = ptr
	db.Unlock()
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) shouldWriteValueToLSM(e *Entry) bool {
	return len(e.Value) < db.opt.ValueThreshold
}

func (db *DB) sendToWriteCh(entries []*Entry) (*request, error) {
	var count, size int64
	for _, e := range entries {
		size += int64(e.estimateSize(db.opt.ValueThreshold))
		count++
	}
	if count >= db.opt.maxBatchCount || size >= db.opt.maxBatchSize {
		return nil, ErrTxnTooBig
	}

	// We can only service one request because we need each txn to be stored in a contigous section.
	// Txns should not interleave among other txns or rewrites.
	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	db.writeCh <- req // Handled in writeWorker.
	y.NumPuts.Add(int64(len(entries)))

	return req, nil
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//   Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*Entry) error {
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

var errNoRoom = errors.New("No room for write")

// ensureRoomForWrite is always called serially.
func (db *DB) ensureRoomForWrite() error {
	if db.mt.MemSize() < db.opt.MaxTableSize {
		return nil
	}
	newMemTable := <-db.memTableCh
	// Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
	err := db.vlog.sync()
	if err != nil {
		return err
	}
	db.Lock()
	defer db.Unlock()
	select {
	case db.flushChan <- flushTask{db.mt, db.vptr}:
		log.Infof("Flushing memtable, mt.size=%d, size of flushChan: %d\n",
			db.mt.MemSize(), len(db.flushChan))
		// We manage to push this task. Let's modify imm.
		db.imm = append(db.imm, db.mt)
		db.mt = newMemTable
		// New memtable is empty. We certainly have room.
		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return errNoRoom
	}
}

func arenaSize(opt Options) int64 {
	return opt.MaxTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func (db *DB) writeLevel0Table(s *skl.Skiplist, f *os.File) error {
	iter := s.NewIterator()
	defer iter.Close()
	b := table.NewTableBuilder(f, db.limiter, db.opt.TableBuilderOptions)
	defer b.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return b.Finish()
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

// TODO: Ensure that this function doesn't return, or is handled by another wrapper function.
// Otherwise, we would have no goroutine which can flush memtables.
func (db *DB) flushMemtable(lc *y.Closer) error {
	defer lc.Done()

	for ft := range db.flushChan {
		if ft.mt == nil {
			return nil
		}

		if !ft.mt.Empty() {
			// Store badger head even if vptr is zero, need it for readTs
			log.Infof("Storing offset: %+v\n", ft.vptr)
			offset := make([]byte, vptrSize)
			ft.vptr.Encode(offset)

			// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
			// commits.
			headTs := y.KeyWithTs(head, db.orc.commitTs())
			ft.mt.Put(headTs, y.ValueStruct{Value: offset})
		}

		fileID := db.lc.reserveFileID()
		fd, err := y.CreateSyncedFile(table.NewFilename(fileID, db.opt.Dir), false)
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

		tbl, err := table.OpenTable(fd, db.opt.TableLoadingMode)
		if err != nil {
			log.Infof("ERROR while opening table: %v", err)
			return err
		}
		// We own a ref on tbl.
		err = db.lc.addLevel0Table(tbl) // This will incrRef (if we don't error, sure)
		tbl.DecrRef()                   // Releases our ref.
		if err != nil {
			return err
		}

		// Update s.imm. Need a lock.
		db.Lock()
		y.Assert(ft.mt == db.imm[0]) //For now, single threaded.
		db.imm = db.imm[1:]
		ft.mt.DecrRef() // Return memory.
		db.Unlock()
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
	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

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
	y.LSMSize.Set(db.opt.Dir, newInt(lsmSize))
	// If valueDir is different from dir, we'd have to do another walk.
	if db.opt.ValueDir != db.opt.Dir {
		_, vlogSize = totalSize(db.opt.ValueDir)
	}
	y.VlogSize.Set(db.opt.Dir, newInt(vlogSize))

}

func (db *DB) updateSize(lc *y.Closer) {
	defer lc.Done()

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected
// duing the session, when DB.PurgeOlderVersions() and DB.PurgeVersions() is
// called. If no such statistics are available, then log files are picked in
// random order. The process stops as soon as the first log file is encountered
// which does not result in garbage collection.
//
// When a log file is picked, it is first sampled If the sample shows that we
// can discard at least discardRatio space of that file, it would be rewritten.
//
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is
// thrown indicating that the call resulted in no file rewrites.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be
// rewritten if half the space can be discarded.  This results in a lifetime
// value log write amplification of 2 (1 from original write + 0.5 rewrite +
// 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
// space reclaims, while setting it to a lower value would result in more space
// reclaims at the cost of increased activity on the LSM tree. discardRatio
// must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
// ErrInvalidRequest is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or DB
// has been closed, this would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM
// tree.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return ErrInvalidRequest
	}

	// Find head on disk
	headKey := y.KeyWithTs(head, math.MaxUint64)
	// Need to pass with timestamp, lsm get removes the last 8 bytes and compares key
	val, err := db.lc.get(headKey)
	if err != nil {
		return errors.Wrap(err, "Retrieving head from on-disk LSM")
	}

	var head valuePointer
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio, head)
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm int64, vlog int64) {
	if y.LSMSize.Get(db.opt.Dir) == nil {
		lsm, vlog = 0, 0
		return
	}
	lsm = y.LSMSize.Get(db.opt.Dir).(*expvar.Int).Value()
	vlog = y.VlogSize.Get(db.opt.Dir).(*expvar.Int).Value()
	return
}

func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}
