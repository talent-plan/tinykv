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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Connor1996/badger/fileutil"
	"github.com/Connor1996/badger/options"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap/errors"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	bitDelete       byte = 1 << 0 // Set if the key has been deleted.
	bitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.

	// The MSB 2 bits are for transactions.
	bitTxn    byte = 1 << 6 // Set if the entry is part of a txn.
	bitFinTxn byte = 1 << 7 // Set if the entry is to indicate end of txn in value log.

	mi int64 = 1 << 20
)

type logFile struct {
	path        string
	fd          *os.File
	fid         uint32
	size        uint32
	loadingMode options.FileLoadingMode
}

// openReadOnly assumes that we have a write lock on logFile.
func (lf *logFile) openReadOnly() error {
	var err error
	lf.fd, err = os.OpenFile(lf.path, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open %q as RDONLY.", lf.path)
	}

	fi, err := lf.fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.path)
	}
	lf.size = uint32(fi.Size())
	return nil
}

func (lf *logFile) doneWriting(offset uint32) error {
	if err := lf.fd.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.path)
	}
	if err := fileutil.Fsync(lf.fd); err != nil {
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.path)
	}
	if err := lf.fd.Close(); err != nil {
		return errors.Wrapf(err, "Unable to close value log: %q", lf.path)
	}

	return lf.openReadOnly()
}

// You must hold lf.lock to sync()
func (lf *logFile) sync() error {
	return fileutil.Fsync(lf.fd)
}

var errStop = errors.New("Stop iteration")
var errTruncate = errors.New("Do truncate")

type logEntry func(e Entry) error

type safeRead struct {
	k  []byte
	v  []byte
	um []byte

	recordOffset uint32
}

func (r *safeRead) Entry(reader *bufio.Reader) (*Entry, error) {
	var hbuf [headerBufSize]byte
	var err error

	hash := crc32.New(y.CastagnoliCrcTable)
	tee := io.TeeReader(reader, hash)
	if _, err = io.ReadFull(tee, hbuf[:]); err != nil {
		return nil, err
	}

	// Encounter preallocated region, just act as EOF.
	if !isEncodedHeader(hbuf[:]) {
		return nil, io.EOF
	}

	var h header
	h.Decode(hbuf[:])
	if h.klen > maxKeySize {
		return nil, errTruncate
	}
	kl := int(h.klen)
	if cap(r.k) < kl {
		r.k = make([]byte, 2*kl)
	}
	vl := int(h.vlen)
	if cap(r.v) < vl {
		r.v = make([]byte, 2*vl)
	}

	e := &Entry{}
	e.offset = r.recordOffset
	e.Key = r.k[:kl]
	e.Value = r.v[:vl]
	if h.umlen > 0 {
		if cap(r.um) < int(h.umlen) {
			r.um = make([]byte, 2*h.umlen)
		}
		e.UserMeta = r.um[:h.umlen]
		if _, err = io.ReadFull(tee, e.UserMeta); err != nil {
			if err == io.EOF {
				err = errTruncate
			}
			return nil, err
		}
	}

	if _, err = io.ReadFull(tee, e.Key); err != nil {
		if err == io.EOF {
			err = errTruncate
		}
		return nil, err
	}
	if _, err = io.ReadFull(tee, e.Value); err != nil {
		if err == io.EOF {
			err = errTruncate
		}
		return nil, err
	}
	var crcBuf [4]byte
	if _, err = io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = errTruncate
		}
		return nil, err
	}
	crc := binary.BigEndian.Uint32(crcBuf[:])
	if crc != hash.Sum32() {
		return nil, errTruncate
	}
	e.meta = h.meta
	return e, nil
}

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (vlog *valueLog) iterate(lf *logFile, offset uint32, fn logEntry) (uint32, error) {
	_, err := lf.fd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return 0, y.Wrap(err)
	}

	reader := bufio.NewReader(lf.fd)
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
	}

	var lastCommit uint64
	validEndOffset := read.recordOffset
	for {
		e, err := read.Entry(reader)
		if err == io.EOF {
			break
		} else if err == io.ErrUnexpectedEOF || err == errTruncate {
			break
		} else if err != nil {
			return validEndOffset, err
		} else if e == nil {
			continue
		}

		read.recordOffset += uint32(headerBufSize + len(e.Key) + len(e.Value) + len(e.UserMeta) + 4) // len(crcBuf)

		if e.meta&bitTxn > 0 {
			txnTs := y.ParseTs(e.Key)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			if lastCommit != txnTs {
				break
			}
		} else if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil || lastCommit != txnTs {
				break
			}
			// Got the end of txn. Now we can store them.
			lastCommit = 0
			validEndOffset = read.recordOffset
		} else {
			if lastCommit != 0 {
				// This is most likely an entry which was moved as part of GC.
				// We shouldn't get this entry in the middle of a transaction.
				break
			}
			validEndOffset = read.recordOffset
		}

		if vlog.opt.ReadOnly {
			return validEndOffset, ErrReplayNeeded
		}
		if err := fn(*e); err != nil {
			if err == errStop {
				break
			}
			return validEndOffset, y.Wrap(err)
		}
	}

	return validEndOffset, nil
}

func (vlog *valueLog) deleteLogFile(lf *logFile) error {
	path := vlog.fpath(lf.fid)
	if err := lf.fd.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}

// lfDiscardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type lfDiscardStats struct {
	sync.Mutex
	m map[uint32]int64
}

type valueLog struct {
	buf        bytes.Buffer
	pendingLen int
	dirPath    string
	curWriter  *fileutil.BufferedWriter
	files      []*logFile

	kv     *DB
	maxPtr uint64

	numEntriesWritten uint32
	opt               Options
	metrics           *y.MetricsSet
}

func vlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.vlog", dirPath, string(os.PathSeparator), fid)
}

func (vlog *valueLog) fpath(fid uint32) string {
	return vlogFilePath(vlog.dirPath, fid)
}

func (vlog *valueLog) currentLogFile() *logFile {
	if len(vlog.files) > 0 {
		return vlog.files[len(vlog.files)-1]
	}
	return nil
}

func (vlog *valueLog) openOrCreateFiles(readOnly bool) error {
	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return errors.Wrapf(err, "Error while opening value log")
	}

	found := make(map[uint64]struct{})
	var maxFid uint32 // Beware len(files) == 0 case, this starts at 0.
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-5], 10, 32)
		if err != nil {
			return errors.Wrapf(err, "Error while parsing value log id for file: %q", file.Name())
		}
		if _, ok := found[fid]; ok {
			return errors.Errorf("Found the same value log file twice: %d", fid)
		}
		found[fid] = struct{}{}

		lf := &logFile{
			fid:         uint32(fid),
			path:        vlog.fpath(uint32(fid)),
			loadingMode: vlog.opt.ValueLogLoadingMode,
		}
		vlog.files = append(vlog.files, lf)
		if uint32(fid) > maxFid {
			maxFid = uint32(fid)
		}
	}
	vlog.maxPtr = uint64(maxFid) << 32
	sort.Slice(vlog.files, func(i, j int) bool {
		return vlog.files[i].fid < vlog.files[j].fid
	})

	// Open all previous log files as read only. Open the last log file
	// as read write (unless the DB is read only).
	for _, lf := range vlog.files {
		if lf.fid == maxFid {
			var flags uint32
			if readOnly {
				flags |= y.ReadOnly
			}
			if lf.fd, err = y.OpenExistingFile(lf.path, flags); err != nil {
				return errors.Wrapf(err, "Unable to open value log file")
			}
			opt := &vlog.opt.ValueLogWriteOptions
			vlog.curWriter = fileutil.NewBufferedWriter(lf.fd, opt.WriteBufferSize, nil)
		} else {
			if err := lf.openReadOnly(); err != nil {
				return err
			}
		}
	}

	// If no files are found, then create a new file.
	if len(vlog.files) == 0 {
		// We already set vlog.maxFid above
		err = vlog.createVlogFile(0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) createVlogFile(fid uint32) error {
	atomic.StoreUint64(&vlog.maxPtr, uint64(fid)<<32)

	path := vlog.fpath(fid)
	lf := &logFile{fid: fid, path: path, loadingMode: vlog.opt.ValueLogLoadingMode}
	vlog.numEntriesWritten = 0

	var err error
	if lf.fd, err = y.CreateSyncedFile(path, false); err != nil {
		return errors.Wrapf(err, "Unable to create value log file")
	}
	if err = fileutil.Preallocate(lf.fd, vlog.opt.ValueLogFileSize); err != nil {
		return errors.Wrap(err, "Unable to preallocate value log file")
	}
	opt := &vlog.opt.ValueLogWriteOptions
	if vlog.curWriter == nil {
		vlog.curWriter = fileutil.NewBufferedWriter(lf.fd, opt.WriteBufferSize, nil)
	} else {
		vlog.curWriter.Reset(lf.fd)
	}

	if err = syncDir(vlog.dirPath); err != nil {
		return errors.Wrapf(err, "Unable to sync value log file dir")
	}
	vlog.files = append(vlog.files, lf)
	syncedFid := atomic.LoadUint32(&vlog.kv.syncedFid)
	for len(vlog.files) > vlog.opt.ValueLogMaxNumFiles {
		deleteCandidate := vlog.files[0]
		if deleteCandidate.fid < syncedFid {
			os.Remove(deleteCandidate.path)
			deleteCandidate.fd.Close()
			vlog.files = vlog.files[1:]
			continue
		}
		break
	}
	return nil
}

func (vlog *valueLog) Open(kv *DB, opt Options) error {
	vlog.dirPath = opt.ValueDir
	vlog.opt = opt
	vlog.kv = kv
	if err := vlog.openOrCreateFiles(kv.opt.ReadOnly); err != nil {
		return errors.Wrapf(err, "Unable to open value log")
	}
	return nil
}

func (vlog *valueLog) Close() error {
	var err error
	for _, f := range vlog.files {
		// A successful close does not guarantee that the data has been successfully saved to disk, as the kernel defers writes.
		// It is not common for a file system to flush the buffers when the stream is closed.
		if syncErr := fileutil.Fdatasync(f.fd); syncErr != nil {
			err = syncErr
		}
		if closeErr := f.fd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// Replay replays the value log. The kv provided is only valid for the lifetime of function call.
func (vlog *valueLog) Replay(off logOffset, fn logEntry) error {
	fid := off.fid
	offset := off.offset
	var lastOffset uint32
	for _, lf := range vlog.files {
		if lf.fid < fid {
			continue
		}
		of := offset
		if lf.fid > fid {
			of = 0
		}
		endAt, err := vlog.iterate(lf, of, fn)
		if err != nil {
			return errors.Wrapf(err, "Unable to replay value log: %q", lf.path)
		}
		if lf.fid == vlog.maxFid() {
			lastOffset = endAt
		}
	}

	// Seek to the end to start writing.
	var err error
	last := vlog.files[len(vlog.files)-1]
	_, err = last.fd.Seek(int64(lastOffset), io.SeekStart)
	atomic.AddUint64(&vlog.maxPtr, uint64(lastOffset))
	return errors.Wrapf(err, "Unable to seek to end of value log: %q", last.path)
}

type logOffset struct {
	fid    uint32
	offset uint32
}

func (lo logOffset) Less(logOff logOffset) bool {
	if lo.fid == logOff.fid {
		return lo.offset < logOff.offset
	}
	return lo.fid < logOff.fid
}

func (lo logOffset) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, lo.fid)
	binary.LittleEndian.PutUint32(buf[4:], lo.offset)
	return buf
}

func (lo *logOffset) Decode(buf []byte) {
	lo.fid = binary.LittleEndian.Uint32(buf)
	lo.offset = binary.LittleEndian.Uint32(buf[4:])
}

type request struct {
	// Input values
	Entries []*Entry
	Wg      sync.WaitGroup
	Err     error
}

func (req *request) Wait() error {
	req.Wg.Wait()
	req.Entries = nil
	err := req.Err
	requestPool.Put(req)
	return err
}

func (vlog *valueLog) getMaxPtr() uint64 {
	return atomic.LoadUint64(&vlog.maxPtr)
}

func (vlog *valueLog) maxFid() uint32 {
	return uint32(atomic.LoadUint64(&vlog.maxPtr) >> 32)
}

func (vlog *valueLog) writableOffset() uint32 {
	return uint32(atomic.LoadUint64(&vlog.maxPtr))
}

func (vlog *valueLog) flush() error {
	curlf := vlog.currentLogFile()
	if vlog.pendingLen == 0 {
		return nil
	}
	err := vlog.curWriter.Flush()
	if err != nil {
		return errors.Wrapf(err, "Unable to write to value log file: %q", curlf.path)
	}
	vlog.metrics.NumWrites.Inc()
	vlog.metrics.NumVLogBytesWritten.Add(float64(vlog.pendingLen))
	atomic.AddUint64(&vlog.maxPtr, uint64(vlog.pendingLen))
	vlog.pendingLen = 0

	if vlog.writableOffset() > uint32(vlog.opt.ValueLogFileSize) ||
		vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries {
		var err error
		if err = curlf.doneWriting(vlog.writableOffset()); err != nil {
			return err
		}
		err = vlog.createVlogFile(vlog.maxFid() + 1)
		if err != nil {
			return err
		}
	}
	return nil
}

// write is thread-unsafe by design and should not be called concurrently.
func (vlog *valueLog) write(reqs []*request) error {
	for i := range reqs {
		b := reqs[i]
		for j := range b.Entries {
			e := b.Entries[j]
			plen, err := encodeEntry(e, &vlog.buf) // Now encode the entry into buffer.
			if err != nil {
				return err
			}
			vlog.curWriter.Append(vlog.buf.Bytes())
			vlog.buf.Reset()
			vlog.pendingLen += plen
			e.logOffset.fid = vlog.currentLogFile().fid
			// Use the offset including buffer length so far.
			e.logOffset.offset = vlog.writableOffset() + uint32(vlog.pendingLen)
		}
		vlog.numEntriesWritten += uint32(len(b.Entries))
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		writeNow :=
			vlog.writableOffset()+uint32(vlog.pendingLen) > uint32(vlog.opt.ValueLogFileSize) ||
				vlog.numEntriesWritten > uint32(vlog.opt.ValueLogMaxEntries)
		if writeNow {
			if err := vlog.flush(); err != nil {
				return err
			}
		}
	}
	return vlog.flush()

	// Acquire mutex locks around this manipulation, so that the reads don't try to use
	// an invalid file descriptor.
}

// Gets the logFile.
func (vlog *valueLog) getFile(fid uint32) (*logFile, error) {
	for i := len(vlog.files) - 1; i >= 0; i-- {
		file := vlog.files[i]
		if file.fid == fid {
			return file, nil
		}
	}
	// log file has gone away, will need to retry the operation.
	return nil, ErrRetry
}
