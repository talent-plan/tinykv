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
	"os"
	"runtime"
	"time"

	"github.com/Connor1996/badger/epoch"
	"github.com/Connor1996/badger/fileutil"
	"github.com/Connor1996/badger/table"
	"github.com/Connor1996/badger/y"
	"github.com/ngaut/log"
)

type writeWorker struct {
	*DB
	writeLSMCh chan postLogTask
	mergeLSMCh chan mergeLSMTask
	flushCh    chan postLogTask
}

type mergeLSMTask struct {
	mt    *table.MemTable
	guard *epoch.Guard
}

type postLogTask struct {
	logFile *os.File
	reqs    []*request
}

func startWriteWorker(db *DB) *y.Closer {
	numWorkers := 3
	if db.opt.SyncWrites {
		numWorkers += 1
	}
	closer := y.NewCloser(numWorkers)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan postLogTask, 1),
		mergeLSMCh: make(chan mergeLSMTask, 1),
		flushCh:    make(chan postLogTask),
	}
	if db.opt.SyncWrites {
		go w.runFlusher(closer)
	}
	go w.runWriteVLog(closer)
	go w.runWriteLSM(closer)
	go w.runMergeLSM(closer)
	return closer
}

func (w *writeWorker) runFlusher(lc *y.Closer) {
	defer lc.Done()
	for {
		select {
		case t := <-w.flushCh:
			start := time.Now()
			err := fileutil.Fdatasync(t.logFile)
			w.metrics.VlogSyncDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				w.done(t.reqs, err)
				continue
			}
			w.writeLSMCh <- t
		case <-lc.HasBeenClosed():
			close(w.writeLSMCh)
			return
		}
	}
}

func (w *writeWorker) runWriteVLog(lc *y.Closer) {
	defer lc.Done()
	for {
		var r *request
		select {
		case task := <-w.ingestCh:
			w.ingestTables(task)
		case r = <-w.writeCh:
			reqs := make([]*request, len(w.writeCh)+1)
			reqs[0] = r
			w.pollWriteCh(reqs[1:])
			if err := w.writeVLog(reqs); err != nil {
				return
			}
		case <-lc.HasBeenClosed():
			w.closeWriteVLog()
			return
		}
	}
}

func (w *writeWorker) pollWriteCh(buf []*request) []*request {
	for i := 0; i < len(buf); i++ {
		buf[i] = <-w.writeCh
	}
	return buf
}

func (w *writeWorker) writeVLog(reqs []*request) error {
	err := w.vlog.write(reqs)
	if err != nil {
		w.done(reqs, err)
		return err
	}
	t := postLogTask{
		logFile: w.vlog.currentLogFile().fd,
		reqs:    reqs,
	}
	if w.opt.SyncWrites {
		w.flushCh <- t
	} else {
		w.writeLSMCh <- t
	}
	return nil
}

func (w *writeWorker) runWriteLSM(lc *y.Closer) {
	defer lc.Done()
	runtime.LockOSThread()
	for {
		t, ok := <-w.writeLSMCh
		if !ok {
			close(w.mergeLSMCh)
			return
		}
		start := time.Now()
		w.writeLSM(t.reqs)
		w.metrics.WriteLSMDuration.Observe(time.Since(start).Seconds())
	}
}

func (w *writeWorker) runMergeLSM(lc *y.Closer) {
	defer lc.Done()
	for task := range w.mergeLSMCh {
		task.mt.MergeListToSkl()
		task.guard.Done()
	}
}

func (w *writeWorker) closeWriteVLog() {
	close(w.writeCh)
	var reqs []*request
	for r := range w.writeCh { // Flush the channel.
		reqs = append(reqs, r)
	}
	err := w.vlog.write(reqs)
	if err != nil {
		w.done(reqs, err)
	} else {
		err = w.vlog.curWriter.Sync()
		// The store is closed, we don't need to write LSM.
		w.done(reqs, err)
	}
	if !w.opt.SyncWrites {
		close(w.writeLSMCh)
	} else {
		// The channel would be closed by the flusher.
	}
}

// writeLSM is called serially by only one goroutine.
func (w *writeWorker) writeLSM(reqs []*request) {
	if len(reqs) == 0 {
		return
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err := w.writeToLSM(b.Entries); err != nil {
			w.done(reqs, err)
			return
		}
	}

	w.done(reqs, nil)
	log.Debugf("%d entries written", count)
	return
}

func (w *writeWorker) done(reqs []*request, err error) {
	for _, r := range reqs {
		r.Err = err
		r.Wg.Done()
	}
	if err != nil {
		log.Warnf("ERROR in Badger::writeLSM: %v", err)
	}
}

func newEntry(entry *Entry) table.Entry {
	return table.Entry{
		Key: entry.Key,
		Value: y.ValueStruct{
			Value:    entry.Value,
			Meta:     entry.meta,
			UserMeta: entry.UserMeta,
			Version:  y.ParseTs(entry.Key),
		},
	}
}

func (w *writeWorker) writeToLSM(entries []*Entry) error {
	for len(entries) != 0 {
		e := newEntry(entries[0])
		free, err := w.ensureRoomForWrite(e.EstimateSize())
		if err != nil {
			return err
		}

		es := make([]table.Entry, 0, len(entries))
		var i int
		for i = 0; i < len(entries); i++ {
			entry := entries[i]
			if entry.meta&bitFinTxn != 0 {
				continue
			}

			e := newEntry(entry)
			if free < e.EstimateSize() {
				break
			}
			free -= e.EstimateSize()
			es = append(es, e)
		}
		w.updateOffset(entries[i-1].logOffset)
		entries = entries[i:]

		w.mt.PutToPendingList(es)
		w.mergeLSMCh <- mergeLSMTask{
			mt:    w.mt,
			guard: w.resourceMgr.Acquire(),
		}
	}

	return nil
}
