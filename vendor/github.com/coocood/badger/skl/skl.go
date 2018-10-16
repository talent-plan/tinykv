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

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package skl

import (
	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coocood/badger/y"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

// MaxNodeSize is the memory footprint of a node of maximum height.
const MaxNodeSize = int(unsafe.Sizeof(node{}))

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-47)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

// Skiplist maps keys to values (in memory)
type Skiplist struct {
	height int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	head   *node
	ref    int32
	arena  *Arena
	rng    rand.Source
}

// IncrRef increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	s.arena.reset()
	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func (s *Skiplist) valid() bool { return s.arena != nil }

func newNode(arena *Arena, key []byte, v y.ValueStruct, height int) *node {
	// The base level is already allocated in the node struct.
	offset := arena.putNode(height)
	node := arena.getNode(offset)
	node.keyOffset = arena.putKey(key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = encodeValue(arena.putVal(v), v.EncodedSize())
	return node
}

func encodeValue(valOffset uint32, valSize uint16) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint16) {
	return uint32(value), uint16(value >> 32)
}

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, y.ValueStruct{}, maxHeight)
	return &Skiplist{
		height: 1,
		head:   head,
		arena:  arena,
		ref:    1,
		rng:    rand.NewSource(time.Now().UnixNano()),
	}
}

func (n *node) getValueOffset() (uint32, uint16) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, v y.ValueStruct) {
	valOffset := arena.putVal(v)
	value := encodeValue(valOffset, v.EncodedSize())
	atomic.StoreUint64(&n.value, value)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// Returns true if key is strictly > n.key.
// If n is nil, this is an "end" marker and we return false.
//func (s *Skiplist) keyIsAfterNode(key []byte, n *node) bool {
//	y.Assert(n != s.head)
//	return n != nil && y.CompareKeys(key, n.key) > 0
//}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && uint32(s.rng.Int63()) <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	var afterNode *node
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		var cmp int
		if next == afterNode {
			// We compared the same node on the upper level, no need to compare again.
			cmp = -1
		} else {
			nextKey := next.key(s.arena)
			cmp = y.CompareKeys(key, nextKey)
		}
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			afterNode = next
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.head {
			return nil, false
		}
		return x, false
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, before, after *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := s.getNext(before, level)
		if next == nil {
			return before, next
		}
		var cmp int
		if next == after {
			// We compared the same node on the upper level, no need to compare again.
			cmp = -1
		} else {
			nextKey := next.key(s.arena)
			cmp = y.CompareKeys(key, nextKey)
		}
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Put inserts the key-value pair.
func (s *Skiplist) Put(key []byte, v y.ValueStruct) {
	s.PutWithHint(key, v, nil)
}

// Hint is used to speed up sequential write.
type Hint struct {
	height int32
	prev   [maxHeight + 1]*node
	next   [maxHeight + 1]*node
}

// PutWithHint inserts the key-value pair with Hint for better sequential write performance.
func (s *Skiplist) PutWithHint(key []byte, v y.ValueStruct, hint *Hint) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.
	listHeight := s.getHeight()
	height := s.randomHeight()

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			listHeight = int32(height)
			break
		}
		listHeight = s.getHeight()
	}
	recomputeHeight := int32(0)
	spliceIsValid := hint != nil
	if hint == nil {
		hint = new(Hint)
	}
	if hint.height < listHeight {
		// Either splice is never used or list height has grown, we recompute all.
		hint.prev[listHeight] = s.head
		hint.next[listHeight] = nil
		hint.height = listHeight
		recomputeHeight = listHeight
	} else {
		for recomputeHeight < listHeight {
			prevNext := s.getNext(hint.prev[recomputeHeight], int(recomputeHeight))
			if prevNext != hint.next[recomputeHeight] {
				recomputeHeight++
			} else if hint.prev[recomputeHeight] != s.head &&
				hint.prev[recomputeHeight] != nil &&
				bytes.Compare(key, hint.prev[recomputeHeight].key(s.arena)) <= 0 {
				// Key is before splice.
				bad := hint.prev[recomputeHeight]
				for bad == hint.prev[recomputeHeight] {
					recomputeHeight++
				}
			} else if hint.next[recomputeHeight] != nil && bytes.Compare(key, hint.next[recomputeHeight].key(s.arena)) > 0 {
				// Key is after splice.
				bad := hint.next[recomputeHeight]
				for bad == hint.next[recomputeHeight] {
					recomputeHeight++
				}
			} else {
				break
			}
		}
	}
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			hint.prev[i], hint.next[i] = s.findSpliceForLevel(key, hint.prev[i+1], hint.next[i+1], int(i))
			if hint.prev[i] == hint.next[i] {
				// In place update.
				match := hint.prev[i]
				match.setValue(s.arena, v)
				for i >= 0 {
					hint.prev[i] = match
					hint.next[i] = match
					i--
				}
				return
			}
		}
	}

	// We do need to create a new node.
	x := newNode(s.arena, key, v, height)

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			nextOffset := s.arena.getNodeOffset(hint.next[i])
			x.tower[i] = nextOffset
			if hint.prev[i].casNextOffset(i, nextOffset, s.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			hint.prev[i], hint.next[i] = s.findSpliceForLevel(key, hint.prev[i], nil, i)
			if i > 0 {
				spliceIsValid = false
			}
		}
	}
	if spliceIsValid {
		for i := 0; i < height; i++ {
			hint.prev[i] = x
		}
	} else {
		hint.height = 0
	}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.head
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *Skiplist) Get(key []byte) y.ValueStruct {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return y.ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !y.SameKey(key, nextKey) {
		return y.ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.Version = y.ParseTs(nextKey)
	return vs
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *Skiplist) NewIterator() *Iterator {
	s.IncrRef()
	return &Iterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *Skiplist
	n    *node
}

// Close frees the resources held by the iterator
func (s *Iterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *Iterator) Value() y.ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// FillValue fills value.
func (s *Iterator) FillValue(vs *y.ValueStruct) {
	valOffset, valSize := s.n.getValueOffset()
	s.list.arena.fillVal(vs, valOffset, valSize)
}

// Next advances to the next position.
func (s *Iterator) Next() {
	y.Assert(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	y.Assert(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *Iterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.head, 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// NewUniIterator returns a UniIterator.
func (s *Skiplist) NewUniIterator(reversed bool) *UniIterator {
	return &UniIterator{
		iter:     s.NewIterator(),
		reversed: reversed,
	}
}

// Next implements y.Interface
func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

// Rewind implements y.Interface
func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

// Seek implements y.Interface
func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

// Key implements y.Interface
func (s *UniIterator) Key() []byte { return s.iter.Key() }

// Value implements y.Interface
func (s *UniIterator) Value() y.ValueStruct { return s.iter.Value() }

// FillValue implements y.Interface
func (s *UniIterator) FillValue(vs *y.ValueStruct) { s.iter.FillValue(vs) }

// Valid implements y.Interface
func (s *UniIterator) Valid() bool { return s.iter.Valid() }

// Close implements y.Interface (and frees up the iter's resources)
func (s *UniIterator) Close() error { return s.iter.Close() }
