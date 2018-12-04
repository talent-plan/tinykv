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

package table

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coocood/badger/options"
	"github.com/coocood/badger/y"
	"github.com/coocood/bbloom"
	"github.com/pkg/errors"
)

const fileSuffix = ".sst"

// Table represents a loaded table file with the info we have about it
type Table struct {
	sync.Mutex

	fd        *os.File // Own fd.
	tableSize int      // Initialized in OpenTable, using fd.Stat().

	blockEndOffsets []uint32
	baseKeys        []byte
	baseKeysEndOffs []uint32

	ref int32 // For file garbage collection.  Atomic.

	loadingMode options.FileLoadingMode
	mmap        []byte // Memory mapped.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys.
	id                uint64 // file id, part of filename

	bf bbloom.Bloom

	hIdx hashIndex
}

// IncrRef increments the refcount (having to do with whether the file should be deleted)
func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *Table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// We can safely delete this file, because for all the current files, we always have
		// at least one reference pointing to them.

		// It's necessary to delete windows files
		if t.loadingMode == options.MemoryMap {
			y.Munmap(t.mmap)
		}
		if err := t.fd.Truncate(0); err != nil {
			// This is very important to let the FS know that the file is deleted.
			return err
		}
		filename := t.fd.Name()
		if err := t.fd.Close(); err != nil {
			return err
		}
		if err := os.Remove(filename); err != nil {
			return err
		}
	}
	return nil
}

type block struct {
	offset int
	data   []byte
}

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(fd *os.File, loadingMode options.FileLoadingMode) (*Table, error) {
	fileInfo, err := fd.Stat()
	if err != nil {
		// It's OK to ignore fd.Close() errs in this function because we have only read
		// from the file.
		_ = fd.Close()
		return nil, y.Wrap(err)
	}

	filename := fileInfo.Name()
	id, ok := ParseFileID(filename)
	if !ok {
		_ = fd.Close()
		return nil, errors.Errorf("Invalid filename: %s", filename)
	}
	t := &Table{
		fd:          fd,
		ref:         1, // Caller is given one reference.
		id:          id,
		loadingMode: loadingMode,
	}

	t.tableSize = int(fileInfo.Size())

	if loadingMode == options.MemoryMap {
		t.mmap, err = y.Mmap(fd, false, fileInfo.Size())
		if err != nil {
			_ = fd.Close()
			return nil, y.Wrapf(err, "Unable to map file")
		}
	} else if loadingMode == options.LoadToRAM {
		err = t.loadToRAM()
		if err != nil {
			_ = fd.Close()
			return nil, y.Wrap(err)
		}
	}

	t.readIndex()

	it := t.NewIterator(false)
	defer it.Close()
	it.Rewind()
	if it.Valid() {
		t.smallest = it.Key()
	}

	it2 := t.NewIterator(true)
	defer it2.Close()
	it2.Rewind()
	if it2.Valid() {
		t.biggest = it2.Key()
	}
	return t, nil
}

// Close closes the open table.  (Releases resources back to the OS.)
func (t *Table) Close() error {
	if t.loadingMode == options.MemoryMap {
		y.Munmap(t.mmap)
	}

	return t.fd.Close()
}

// PointGet try to lookup a key and its value by table's hash index.
// If it find an hash collision the last return value will be false,
// which means caller should fallback to seek search. Otherwise it value will be true.
// If the hash index does not contain such an element the returned key will be nil.
func (t *Table) PointGet(key []byte) ([]byte, y.ValueStruct, bool) {
	keyNoTS := y.ParseKey(key)
	blkIdx, offset := t.hIdx.lookup(keyNoTS)
	if blkIdx == resultFallback {
		return nil, y.ValueStruct{}, false
	}
	if blkIdx == resultNoEntry {
		return nil, y.ValueStruct{}, true
	}

	it := t.NewIteratorNoRef(false)
	it.seekFromOffset(int(blkIdx), int(offset), key)

	if !it.Valid() || !y.SameKey(key, it.Key()) {
		return nil, y.ValueStruct{}, true
	}
	return it.Key(), it.Value(), true
}

func (t *Table) read(off int, sz int) ([]byte, error) {
	if len(t.mmap) > 0 {
		if len(t.mmap[off:]) < sz {
			return nil, y.ErrEOF
		}
		return t.mmap[off : off+sz], nil
	}

	res := make([]byte, sz)
	nbr, err := t.fd.ReadAt(res, int64(off))
	y.NumReads.Add(1)
	y.NumBytesRead.Add(int64(nbr))
	return res, err
}

func (t *Table) readNoFail(off int, sz int) []byte {
	res, err := t.read(off, sz)
	y.Check(err)
	return res
}

func (t *Table) readIndex() {
	readPos := t.tableSize

	readPos -= 4
	buf := t.readNoFail(readPos, 4)
	numBuckets := int(bytesToU32(buf))
	if numBuckets != 0 {
		hashLen := numBuckets * 3
		readPos -= hashLen
		buckets := t.readNoFail(readPos, hashLen)
		t.hIdx.readIndex(buckets, numBuckets)
	}

	// Read bloom filter.
	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	bloomLen := int(bytesToU32(buf))
	readPos -= bloomLen
	data := t.readNoFail(readPos, bloomLen)
	t.bf.BinaryUnmarshal(data)

	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	numBlocks := int(bytesToU32(buf))

	readPos -= 4 * numBlocks
	buf = t.readNoFail(readPos, 4*numBlocks)
	t.baseKeysEndOffs = bytesToU32Slice(buf)

	baseKeyBufLen := int(t.baseKeysEndOffs[numBlocks-1])
	readPos -= baseKeyBufLen
	t.baseKeys = t.readNoFail(readPos, baseKeyBufLen)

	readPos -= 4 * numBlocks
	buf = t.readNoFail(readPos, 4*numBlocks)
	t.blockEndOffsets = bytesToU32Slice(buf)
}

func (t *Table) block(idx int) (block, error) {
	y.Assert(idx >= 0)
	if idx >= len(t.blockEndOffsets) {
		return block{}, errors.New("block out of index")
	}
	var startOffset int
	if idx > 0 {
		startOffset = int(t.blockEndOffsets[idx-1])
	}
	endOffset := int(t.blockEndOffsets[idx])
	blk := block{
		offset: startOffset,
	}
	var err error
	blk.data, err = t.read(startOffset, endOffset-startOffset)
	return blk, err
}

// Size is its file size in bytes
func (t *Table) Size() int64 { return int64(t.tableSize) }

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() []byte { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() []byte { return t.biggest }

// Filename is NOT the file name.  Just kidding, it is.
func (t *Table) Filename() string { return t.fd.Name() }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.id }

// DoesNotHave returns true if (but not "only if") the table does not have the key.  It does a
// bloom filter lookup.
func (t *Table) DoesNotHave(key []byte) bool { return !t.bf.Has(key) }

// ParseFileID reads the file id out of a filename.
func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	y.Assert(id >= 0)
	return uint64(id), true
}

// IDToFilename does the inverse of ParseFileID
func IDToFilename(id uint64) string {
	return fmt.Sprintf("%06d", id) + fileSuffix
}

// NewFilename should be named TableFilepath -- it combines the dir with the ID to make a table
// filepath.
func NewFilename(id uint64, dir string) string {
	return filepath.Join(dir, IDToFilename(id))
}

func (t *Table) loadToRAM() error {
	t.mmap = make([]byte, t.tableSize)
	read, err := t.fd.ReadAt(t.mmap, 0)
	if err != nil || read != t.tableSize {
		return y.Wrapf(err, "Unable to load file in memory. Table file: %s", t.Filename())
	}
	y.NumReads.Add(1)
	y.NumBytesRead.Add(int64(read))
	return nil
}
