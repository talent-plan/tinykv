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
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/Connor1996/badger/surf"
	"github.com/Connor1996/badger/y"
)

var maxGlobalTs = [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

type blockIterator struct {
	data    []byte
	idx     int
	err     error
	baseKey []byte

	globalTs [8]byte
	key      []byte
	val      []byte

	lastBaseLen     uint16
	entryEndOffsets []uint32
}

func (itr *blockIterator) setBlock(b block) {
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.lastBaseLen = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	itr.data = b.data
	itr.loadEntryEndOffsets()
}

func (itr *blockIterator) valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}

// loadEntryEndOffsets loads the entryEndOffsets for binary searching for a key.
func (itr *blockIterator) loadEntryEndOffsets() {
	// Get the number of entries from the end of `data` (and remove it).
	off := len(itr.data) - 4
	entriesNum := int(bytesToU32(itr.data[off:]))
	itr.data = itr.data[:off]
	off = len(itr.data) - entriesNum*4
	itr.entryEndOffsets = bytesToU32Slice(itr.data[off:])
	itr.data = itr.data[:off]
}

// Seek brings us to the first block element that is >= input key.
// The binary search will begin at `start`, you can use it to skip some items.
func (itr *blockIterator) seek(key []byte) {
	foundEntryIdx := sort.Search(len(itr.entryEndOffsets), func(idx int) bool {
		itr.setIdx(idx)
		return y.CompareKeysWithVer(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// seekToFirst brings us to the first element. Valid should return true.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

// seekToLast brings us to the last element. Valid should return true.
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryEndOffsets) - 1)
}

// setIdx sets the iterator to the entry index and set the current key and value.
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryEndOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	var startOffset int
	if i > 0 {
		startOffset = int(itr.entryEndOffsets[i-1])
	}

	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.Decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diffLen]
	}

	endOffset := int(itr.entryEndOffsets[i])
	entryData := itr.data[startOffset:endOffset]
	var h header
	h.Decode(entryData)
	if h.baseLen > itr.lastBaseLen {
		itr.key = append(itr.key[:itr.lastBaseLen], itr.baseKey[itr.lastBaseLen:h.baseLen]...)
	}
	itr.lastBaseLen = h.baseLen
	valueOff := headerSize + int(h.diffLen)
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.baseLen], diffKey...)
	if itr.globalTs != maxGlobalTs {
		itr.key = append(itr.key, itr.globalTs[:]...)
	}
	itr.val = entryData[valueOff:]
}

func (itr *blockIterator) next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) prev() {
	itr.setIdx(itr.idx - 1)
}

// Iterator is an iterator for a Table.
type Iterator struct {
	t    *Table
	surf *surf.Iterator
	bpos int
	bi   blockIterator
	err  error

	// Internally, Iterator is bidirectional. However, we only expose the
	// unidirectional functionality for now.
	reversed bool
}

// NewIterator returns a new iterator of the Table
func (t *Table) NewIterator(reversed bool) *Iterator {
	it := &Iterator{t: t, reversed: reversed}
	binary.BigEndian.PutUint64(it.bi.globalTs[:], t.globalTs)
	if t.surf != nil {
		it.surf = t.surf.NewIterator()
	}
	return it
}

func (itr *Iterator) reset() {
	itr.bpos = 0
	itr.err = nil
}

// Valid follows the y.Iterator interface
func (itr *Iterator) Valid() bool {
	return itr.err == nil
}

func (itr *Iterator) seekToFirst() {
	numBlocks := len(itr.t.blockEndOffsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToFirst()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekToLast() {
	numBlocks := len(itr.t.blockEndOffsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = numBlocks - 1
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToLast()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekHelper(blockIdx int, key []byte) {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seek(key)
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekFromOffset(blockIdx int, offset int, key []byte) {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.setIdx(offset)
	if y.CompareKeysWithVer(itr.bi.key, key) >= 0 {
		return
	}
	itr.bi.seek(key)
	itr.err = itr.bi.err
}

func (itr *Iterator) seekBlock(key []byte) int {
	return sort.Search(len(itr.t.blockEndOffsets), func(idx int) bool {
		baseKeyStartOff := 0
		if idx > 0 {
			baseKeyStartOff = int(itr.t.baseKeysEndOffs[idx-1])
		}
		baseKeyEndOff := itr.t.baseKeysEndOffs[idx]
		baseKey := itr.t.baseKeys[baseKeyStartOff:baseKeyEndOff]
		if itr.bi.globalTs != maxGlobalTs {
			cmp := bytes.Compare(baseKey, y.ParseKey(key))
			if cmp != 0 {
				return cmp > 0
			}
			return bytes.Compare(itr.bi.globalTs[:], key[len(key)-8:]) > 0
		}
		return y.CompareKeysWithVer(baseKey, key) > 0
	})
}

// seekFrom brings us to a key that is >= input key.
func (itr *Iterator) seekFrom(key []byte) {
	itr.err = nil
	itr.reset()

	idx := itr.seekBlock(key)
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		itr.seekHelper(0, key)
		return
	}

	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	itr.seekHelper(idx-1, key)
	if itr.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(itr.t.blockEndOffsets) {
			// If idx == len(itr.t.blockEndOffsets), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekHelper(idx, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

// seek will reset iterator and seek to >= key.
func (itr *Iterator) seek(key []byte) {
	itr.err = nil
	itr.reset()

	if itr.surf == nil {
		itr.seekFrom(key)
		return
	}

	sit := itr.surf
	sit.Seek(y.ParseKey(key))
	if !sit.Valid() {
		itr.err = io.EOF
		return
	}

	var pos entryPosition
	pos.decode(sit.Value())
	itr.seekFromOffset(int(pos.blockIdx), int(pos.offset), key)
}

// seekForPrev will reset iterator and seek to <= key.
func (itr *Iterator) seekForPrev(key []byte) {
	// TODO: Optimize this. We shouldn't have to take a Prev step.
	itr.seekFrom(key)
	if !bytes.Equal(itr.Key(), key) {
		itr.prev()
	}
}

func (itr *Iterator) next() {
	itr.err = nil

	if itr.bpos >= len(itr.t.blockEndOffsets) {
		itr.err = io.EOF
		return
	}

	if itr.bi.data == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToFirst()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.next()
	if !itr.bi.valid() {
		itr.bpos++
		itr.bi.data = nil
		itr.next()
		return
	}
}

func (itr *Iterator) prev() {
	itr.err = nil
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	if itr.bi.data == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToLast()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.prev()
	if !itr.bi.valid() {
		itr.bpos--
		itr.bi.data = nil
		itr.prev()
		return
	}
}

// Key follows the y.Iterator interface
func (itr *Iterator) Key() []byte {
	return itr.bi.key
}

func (itr *Iterator) RawKey() []byte {
	return y.ParseKey(itr.bi.key)
}

// Value follows the y.Iterator interface
func (itr *Iterator) Value() (ret y.ValueStruct) {
	ret.Decode(itr.bi.val)
	return
}

// FillValue fill the value struct.
func (itr *Iterator) FillValue(vs *y.ValueStruct) {
	vs.Decode(itr.bi.val)
}

// Next follows the y.Iterator interface
func (itr *Iterator) Next() {
	if !itr.reversed {
		itr.next()
	} else {
		itr.prev()
	}
}

// Rewind follows the y.Iterator interface
func (itr *Iterator) Rewind() {
	if !itr.reversed {
		itr.seekToFirst()
	} else {
		itr.seekToLast()
	}
}

// Seek follows the y.Iterator interface
func (itr *Iterator) Seek(key []byte) {
	if !itr.reversed {
		itr.seek(key)
	} else {
		itr.seekForPrev(key)
	}
}

// ConcatIterator concatenates the sequences defined by several iterators.  (It only works with
// TableIterators, probably just because it's faster to not be so generic.)
type ConcatIterator struct {
	idx      int // Which iterator is active now.
	cur      *Iterator
	iters    []*Iterator // Corresponds to tables.
	tables   []*Table    // Disregarding reversed, this is in ascending order.
	reversed bool
}

// NewConcatIterator creates a new concatenated iterator
func NewConcatIterator(tbls []*Table, reversed bool) *ConcatIterator {
	return &ConcatIterator{
		reversed: reversed,
		iters:    make([]*Iterator, len(tbls)),
		tables:   tbls,
		idx:      -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
	} else {
		if s.iters[s.idx] == nil {
			// We already increased table refs, so init without IncrRef here
			ti := s.tables[s.idx].NewIterator(s.reversed)
			ti.next()
			s.iters[s.idx] = ti
		}
		s.cur = s.iters[s.idx]
	}
}

// Rewind implements y.Interface
func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.reversed {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

// Valid implements y.Interface
func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

// Key implements y.Interface
func (s *ConcatIterator) Key() []byte {
	return s.cur.Key()
}

// Value implements y.Interface
func (s *ConcatIterator) Value() y.ValueStruct {
	return s.cur.Value()
}

func (s *ConcatIterator) FillValue(vs *y.ValueStruct) {
	s.cur.FillValue(vs)
}

// Seek brings us to element >= key if reversed is false. Otherwise, <= key.
func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if !s.reversed {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return y.CompareKeysWithVer(s.tables[i].Biggest(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return y.CompareKeysWithVer(s.tables[n-1-i].Smallest(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.reversed {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}
