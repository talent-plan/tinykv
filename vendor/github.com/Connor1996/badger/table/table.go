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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/DataDog/zstd"
	"github.com/Connor1996/badger/fileutil"
	"github.com/Connor1996/badger/options"
	"github.com/Connor1996/badger/surf"
	"github.com/Connor1996/badger/y"
	"github.com/coocood/bbloom"
	"github.com/dgraph-io/ristretto"
	"github.com/golang/snappy"
	"github.com/pingcap/errors"
)

const fileSuffix = ".sst"
const intSize = int(unsafe.Sizeof(int(0)))

// Table represents a loaded table file with the info we have about it
type Table struct {
	sync.Mutex

	fd        *os.File // Own fd.
	tableSize int      // Initialized in OpenTable, using fd.Stat().

	globalTs        uint64
	blockEndOffsets []uint32
	baseKeys        []byte
	baseKeysEndOffs []uint32

	loadingMode options.FileLoadingMode
	mmap        []byte // Memory mapped.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys.
	id                uint64 // file id, part of filename

	compacting int32

	bf   *bbloom.Bloom
	hIdx *hashIndex
	surf *surf.SuRF

	compression options.CompressionType

	cache *ristretto.Cache
}

// CompressionType returns the compression algorithm used for block compression.
func (t *Table) CompressionType() options.CompressionType {
	return t.compression
}

// Delete delete table's file from disk.
func (t *Table) Delete() error {
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
	return os.Remove(filename)
}

type block struct {
	offset int
	data   []byte
}

func (b *block) size() int64 {
	return int64(intSize + len(b.data))
}

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(fd *os.File, loadingMode options.FileLoadingMode, compression options.CompressionType, cache *ristretto.Cache) (*Table, error) {
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
		id:          id,
		loadingMode: loadingMode,
		compression: compression,
		cache:       cache,
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
	it.Rewind()
	if it.Valid() {
		// key with max ts is the binary smallest key.
		t.smallest = y.KeyWithTs(it.RawKey(), math.MaxUint64)
	}

	it2 := t.NewIterator(true)
	it2.Rewind()
	if it2.Valid() {
		// key with 0 ts is the binary biggest key.
		t.biggest = y.KeyWithTs(it2.RawKey(), 0)
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
func (t *Table) PointGet(key []byte, keyHash uint64) ([]byte, y.ValueStruct, bool) {
	if t.bf != nil && !t.bf.Has(keyHash) {
		return nil, y.ValueStruct{}, true
	}

	blkIdx, offset := uint32(resultFallback), uint8(0)
	if t.hIdx != nil {
		blkIdx, offset = t.hIdx.lookup(keyHash)
	} else if t.surf != nil {
		v, ok := t.surf.Get(y.ParseKey(key))
		if !ok {
			blkIdx = resultNoEntry
		} else {
			var pos entryPosition
			pos.decode(v)
			blkIdx, offset = uint32(pos.blockIdx), pos.offset
		}
	}
	if blkIdx == resultFallback {
		return nil, y.ValueStruct{}, false
	}
	if blkIdx == resultNoEntry {
		return nil, y.ValueStruct{}, true
	}

	it := t.NewIterator(false)
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
	_, err := t.fd.ReadAt(res, int64(off))
	return res, err
}

func (t *Table) readNoFail(off int, sz int) []byte {
	res, err := t.read(off, sz)
	y.Check(err)
	return res
}

func (t *Table) readIndex() {
	readPos := t.tableSize

	readPos -= 8
	buf := t.readNoFail(readPos, 8)
	t.globalTs = binary.BigEndian.Uint64(buf)

	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	surfSize := int(bytesToU32(buf))
	if surfSize != 0 {
		readPos -= surfSize
		data := t.readNoFail(readPos, surfSize)
		t.surf = new(surf.SuRF)
		t.surf.Unmarshal(data)
	}

	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	numBuckets := int(bytesToU32(buf))
	if numBuckets != 0 {
		hashLen := numBuckets * 3
		readPos -= hashLen
		buckets := t.readNoFail(readPos, hashLen)
		t.hIdx = new(hashIndex)
		t.hIdx.readIndex(buckets, numBuckets)
	}

	// Read bloom filter.
	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	bloomLen := int(bytesToU32(buf))
	if bloomLen != 0 {
		readPos -= bloomLen
		data := t.readNoFail(readPos, bloomLen)
		t.bf = new(bbloom.Bloom)
		t.bf.BinaryUnmarshal(data)
	}

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
	if t.cache != nil {
		key := t.blockCacheKey(idx)
		blk, ok := t.cache.Get(key)
		if ok && blk != nil {
			return blk.(block), nil
		}
	}
	blk := block{
		offset: startOffset,
	}
	endOffset := int(t.blockEndOffsets[idx])
	dataLen := endOffset - startOffset
	var err error
	if blk.data, err = t.read(blk.offset, dataLen); err != nil {
		return block{}, errors.Wrapf(err,
			"failed to read from file: %s at offset: %d, len: %d", t.fd.Name(), blk.offset, dataLen)
	}

	blk.data, err = t.decompressData(blk.data)
	if err != nil {
		return block{}, errors.Wrapf(err,
			"failed to decode compressed data in file: %s at offset: %d, len: %d",
			t.fd.Name(), blk.offset, dataLen)
	}
	if t.cache != nil {
		key := t.blockCacheKey(idx)
		t.cache.Set(key, blk, blk.size())
	}
	return blk, nil
}

func (t *Table) approximateOffset(it *Iterator, key []byte) int {
	if y.CompareKeysWithVer(t.Biggest(), key) < 0 {
		return int(t.blockEndOffsets[len(t.blockEndOffsets)-1])
	} else if y.CompareKeysWithVer(t.Smallest(), key) > 0 {
		return 0
	}
	blk := it.seekBlock(key)
	if blk != 0 {
		return int(t.blockEndOffsets[blk-1])
	}
	return 0
}

// HasGlobalTs returns table does set global ts.
func (t *Table) HasGlobalTs() bool {
	return t.globalTs != math.MaxUint64
}

// SetGlobalTs update the global ts of external ingested tables.
func (t *Table) SetGlobalTs(ts uint64) error {
	var buf [8]byte
	encodeTs := math.MaxUint64 - ts
	binary.BigEndian.PutUint64(buf[:], encodeTs)
	if _, err := t.fd.WriteAt(buf[:], t.Size()-8); err != nil {
		return err
	}
	if err := fileutil.Fsync(t.fd); err != nil {
		return err
	}
	t.globalTs = encodeTs
	return nil
}

func (t *Table) MarkCompacting(flag bool) {
	if flag {
		atomic.StoreInt32(&t.compacting, 1)
	}
	atomic.StoreInt32(&t.compacting, 0)
}

func (t *Table) IsCompacting() bool {
	return atomic.LoadInt32(&t.compacting) == 1
}

func (t *Table) blockCacheKey(idx int) uint64 {
	y.Assert(t.ID() < math.MaxUint32)
	y.Assert(idx < math.MaxUint32)
	return (t.ID() << 32) | uint64(idx)
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

func (t *Table) HasOverlap(start, end []byte, includeEnd bool) bool {
	if y.CompareKeysWithVer(start, t.Biggest()) > 0 {
		return false
	}

	if cmp := y.CompareKeysWithVer(end, t.Smallest()); cmp < 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}

	if t.surf != nil {
		return t.surf.HasOverlap(start, end, includeEnd)
	}

	it := t.NewIterator(false)
	it.Seek(start)
	if !it.Valid() {
		return false
	}
	if cmp := y.CompareKeysWithVer(it.Key(), end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

// ParseFileID reads the file id out of a filename.
func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// IDToFilename does the inverse of ParseFileID
func IDToFilename(id uint64) string {
	return fmt.Sprintf("%08x", id) + fileSuffix
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
	return nil
}

// decompressData decompresses the given data.
func (t *Table) decompressData(data []byte) ([]byte, error) {
	switch t.compression {
	case options.None:
		return data, nil
	case options.Snappy:
		return snappy.Decode(nil, data)
	case options.ZSTD:
		return zstd.Decompress(nil, data)
	}
	return nil, errors.New("Unsupported compression type")
}
