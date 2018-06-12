package lockstore

import (
	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

// LockStore is a skiplist variant used to store lock.
// Compares to normal skip list, it only support Insert and Delete operation.
// and only support single thread write.
// But it can reuse the memory, so that the memory usage doesn't keep growing.
type LockStore struct {
	height   int32 // Current height. 1 <= height <= kMaxHeight.
	head     *node
	arenaPtr unsafe.Pointer

	// We only consume 2 bits for a random height call.
	rand rand.Source64
}

const (
	maxHeight     = 16
	nodeHeadrSize = int(unsafe.Sizeof(nodeHeader{}))
)

type nodeHeader struct {
	addr   arenaAddr
	height uint16
	keyLen uint16
	valLen uint32
}

type node struct {
	nodeHeader
	// Height of the nexts.

	nexts [maxHeight]uint64
}

type entry struct {
	*node
	key []byte
}

func (e *entry) getValue(arena *arena) []byte {
	if e.node != nil {
		return e.node.getValue(arena)
	}
	return nil
}

func (n *node) getNextAddr(level int) arenaAddr {
	return arenaAddr(atomic.LoadUint64(&n.nexts[level]))
}

func (n *node) setNextAddr(level int, addr arenaAddr) {
	atomic.StoreUint64(&n.nexts[level], uint64(addr))
}

func (n *node) entryLen() int {
	return n.nodeLen() + int(n.keyLen) + int(n.valLen)
}

func (n *node) nodeLen() int {
	return int(n.height)*8 + nodeHeadrSize
}

func (n *node) getKey(a *arena) []byte {
	nodeLen := n.nodeLen()
	entryData := a.get(n.addr, nodeLen+int(n.keyLen))
	return entryData[nodeLen:]
}

func (n *node) getValue(a *arena) []byte {
	nodeLenKeyLen := n.nodeLen() + int(n.keyLen)
	entryData := a.get(n.addr, nodeLenKeyLen+int(n.valLen))
	return entryData[nodeLenKeyLen:]
}

func NewLockStore(arenaBlockSize int) *LockStore {
	return &LockStore{
		height:   1,
		head:     new(node),
		arenaPtr: unsafe.Pointer(newArenaLocator(arenaBlockSize)),
		rand:     rand.NewSource(time.Now().Unix()).(rand.Source64),
	}
}

func (ls *LockStore) getHeight() int {
	return int(atomic.LoadInt32(&ls.height))
}

func (ls *LockStore) setHeight(height int) {
	atomic.StoreInt32(&ls.height, int32(height))
}

func (ls *LockStore) Get(key, buf []byte) []byte {
	e, match := ls.findGreater(key, true)
	if !match {
		return nil
	}
	e.getValue(ls.getArena())
	return append(buf[:0], e.getValue(ls.getArena())...)
}

func (ls *LockStore) getNext(n *node, level int) (e entry) {
	addr := n.getNextAddr(level)
	if addr == nullArenaAddr {
		return
	}
	arena := ls.getArena()
	data := arena.get(addr, nodeHeadrSize)
	e.node = (*node)(unsafe.Pointer(&data[0]))
	e.key = e.node.getKey(arena)
	return e
}

func (ls *LockStore) findGreater(key []byte, allowEqual bool) (entry, bool) {
	var prev entry
	prev.node = ls.head
	level := ls.getHeight() - 1
	for {
		var next entry
		addr := prev.getNextAddr(level)
		if addr != nullArenaAddr {
			arena := ls.getArena()
			data := arena.get(addr, nodeHeadrSize)
			next.node = (*node)(unsafe.Pointer(&data[0]))
			next.key = next.node.getKey(arena)
			cmp := bytes.Compare(next.key, key)
			if cmp < 0 {
				// next key is still smaller, keep moving.
				prev = next
				continue
			}
			if cmp == 0 {
				// prev.key < key == next.key.
				if allowEqual {
					return next, true
				}
				level = 0
				prev = next
				continue
			}
		}
		// next is greater than key or next is nil. go to the lower level.
		if level > 0 {
			level--
			continue
		}
		return next, false
	}
}

func (ls *LockStore) findLess(key []byte, allowEqual bool) (entry, bool) {
	var prev entry
	prev.node = ls.head
	level := ls.getHeight() - 1
	for {
		next := ls.getNext(prev.node, level)
		if next.node != nil {
			cmp := bytes.Compare(key, next.key)
			if cmp > 0 {
				// prev.key < next.key < key. We can continue to move right.
				prev = next
				continue
			}
			if cmp == 0 && allowEqual {
				// prev.key < key == next.key.
				return next, true
			}
		}
		// get closer to the key in the lower level.
		if level > 0 {
			level--
			continue
		}
		break
	}
	// We are not going to return head.
	if prev.node == ls.head {
		return entry{}, false
	}
	return prev, false
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key < key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return true.
func (ls *LockStore) findSpliceForLevel(arena *arena, key []byte, before *node, level int) (*node, *node, bool) {
	for {
		// Assume before.key < key.
		nextAddr := before.getNextAddr(level)
		if nextAddr == nullArenaAddr {
			return before, nil, false
		}
		data := arena.get(nextAddr, nodeHeadrSize)
		next := (*node)(unsafe.Pointer(&data[0]))
		nextKey := next.getKey(arena)
		cmp := bytes.Compare(nextKey, key)
		if cmp >= 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next, cmp == 0
		}
		before = next // Keep moving right on this level.
	}
}

// findLast returns the last element. If head (empty ls), we return nil. All the find functions
// will NEVER return the head nodes.
func (ls *LockStore) findLast() entry {
	var e entry
	e.node = ls.head
	level := ls.getHeight() - 1
	for {
		next := ls.getNext(e.node, level)
		if next.node != nil {
			e = next
			continue
		}
		if level == 0 {
			if e.node == ls.head {
				return entry{}
			}
			return e
		}
		level--
	}
}

func (ls *LockStore) getNode(arena *arena, addr arenaAddr) *node {
	data := arena.get(addr, nodeHeadrSize)
	return (*node)(unsafe.Pointer(&data[0]))
}

// Put inserts the key-value pair.
func (ls *LockStore) Insert(key []byte, v []byte) bool {
	arena := ls.getArena()
	lsHeight := ls.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[lsHeight] = ls.head
	for i := int(lsHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		var exists bool
		prev[i], next[i], exists = ls.findSpliceForLevel(ls.getArena(), key, prev[i+1], i)
		if exists {
			// The save key already exists.
			return false
		}
	}
	height := ls.randomHeight()
	x := ls.newNode(arena, key, v, height)
	if height > int(lsHeight) {
		ls.setHeight(height)
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		if next[i] != nil {
			x.nexts[i] = uint64(next[i].addr)
		} else {
			x.nexts[i] = uint64(nullArenaAddr)
		}
		if prev[i] == nil {
			prev[i] = ls.head
		}
		prev[i].setNextAddr(i, x.addr)
	}
	return true
}

func (ls *LockStore) newNode(arena *arena, key []byte, v []byte, height int) *node {
	// The base level is already allocated in the node struct.
	nodeSize := int(nodeHeadrSize) + height*8 + len(key) + len(v)
	addr := arena.alloc(nodeSize)
	if addr == nullArenaAddr {
		arena = arena.grow()
		ls.setArena(arena)
		// The new arena block must have enough memory to alloc.
		addr = arena.alloc(nodeSize)
	}
	data := arena.get(addr, nodeSize)
	node := (*node)(unsafe.Pointer(&data[0]))
	node.addr = addr
	node.keyLen = uint16(len(key))
	node.height = uint16(height)
	node.valLen = uint32(len(v))
	copy(data[node.nodeLen():], key)
	copy(data[node.nodeLen()+int(node.keyLen):], v)
	return node
}

func (ls *LockStore) getArena() *arena {
	return (*arena)(atomic.LoadPointer(&ls.arenaPtr))
}

func (ls *LockStore) setArena(al *arena) {
	atomic.StorePointer(&ls.arenaPtr, unsafe.Pointer(al))
}

func (ls *LockStore) randomHeight() int {
	h := 1
	for h < maxHeight && ls.rand.Uint64() < uint64(math.MaxUint64)/4 {
		h++
	}
	return h
}

func (ls *LockStore) Delete(key []byte) bool {
	listHeight := ls.getHeight()
	var prevs [maxHeight + 1]*node
	prevs[listHeight] = ls.head
	var keyNode *node
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		var match bool
		prevs[i], keyNode, match = ls.findSpliceForLevel(ls.getArena(), key, prevs[i+1], i)
		if !match {
			keyNode = nil
		}
	}
	if keyNode == nil {
		return false
	}
	for i := int(keyNode.height) - 1; i >= 0; i-- {
		// Change the nexts from higher to lower, so the data is consistent at any point.
		prevs[i].setNextAddr(i, keyNode.getNextAddr(i))
	}
	ls.getArena().free(keyNode.addr)
	return true
}
