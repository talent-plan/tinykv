package table

import (
	"bytes"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/Connor1996/badger/skl"
	"github.com/Connor1996/badger/y"
)

type Entry struct {
	Key   []byte
	Value y.ValueStruct
}

func (e *Entry) EstimateSize() int64 {
	return int64(len(e.Key) + int(e.Value.EncodedSize()) + skl.EstimateNodeSize)
}

type MemTable struct {
	skl         *skl.Skiplist
	pendingList unsafe.Pointer // *listNode
}

func NewMemTable(arenaSize int64) *MemTable {
	return &MemTable{skl: skl.NewSkiplist(arenaSize)}
}

func (mt *MemTable) Delete() error {
	mt.skl.Delete()
	return nil
}

func (mt *MemTable) Empty() bool {
	return atomic.LoadPointer(&mt.pendingList) == nil && mt.skl.Empty()
}

func (mt *MemTable) Get(key []byte) y.ValueStruct {
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		if v, ok := curr.get(key); ok {
			return v
		}
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}
	return mt.skl.Get(key)
}

func (mt *MemTable) NewIterator(reverse bool) y.Iterator {
	var (
		sklItr = mt.skl.NewUniIterator(reverse)
		its    []y.Iterator
	)
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		its = append(its, curr.newIterator(reverse))
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}

	if len(its) == 0 {
		return sklItr
	}
	its = append(its, sklItr)
	return NewMergeIterator(its, reverse)
}

func (mt *MemTable) MemSize() int64 {
	var sz int64
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		sz += curr.memSize
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}
	return mt.skl.MemSize() + sz
}

// PutToSkl directly insert entry into SkipList.
func (mt *MemTable) PutToSkl(key []byte, v y.ValueStruct) {
	mt.skl.Put(key, v)
}

// PutToPendingList put entries to pending list, and you can call MergeListToSkl to merge them to SkipList later.
func (mt *MemTable) PutToPendingList(entries []Entry) {
	mt.putToList(entries)
}

// MergeListToSkl merge all entries in pending list to SkipList.
func (mt *MemTable) MergeListToSkl() {
	head := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	if head == nil {
		return
	}

	head.mergeToSkl(mt.skl)
	// No new node inserted, just update head of list.
	if atomic.CompareAndSwapPointer(&mt.pendingList, unsafe.Pointer(head), nil) {
		return
	}
	// New node inserted, iterate to find `prev` of old head.
	curr := (*listNode)(atomic.LoadPointer(&mt.pendingList))
	for curr != nil {
		next := atomic.LoadPointer(&curr.next)
		if unsafe.Pointer(head) == next {
			atomic.StorePointer(&curr.next, nil)
			return
		}
		curr = (*listNode)(next)
	}
}

func (mt *MemTable) putToList(entries []Entry) {
	n := newListNode(entries)
	for {
		old := atomic.LoadPointer(&mt.pendingList)
		n.next = old
		if atomic.CompareAndSwapPointer(&mt.pendingList, old, unsafe.Pointer(n)) {
			return
		}
	}
}

type listNode struct {
	next    unsafe.Pointer // *listNode
	entries []Entry
	memSize int64
}

func newListNode(entries []Entry) *listNode {
	n := &listNode{entries: entries}
	for _, e := range n.entries {
		sz := e.EstimateSize()
		n.memSize += sz
	}
	for _, e := range entries {
		e.Value.Version = y.ParseTs(e.Key)
	}
	return n
}

func (n *listNode) putToSkl(s *skl.Skiplist, entries []Entry) {
	var hint skl.Hint
	for _, e := range entries {
		s.PutWithHint(e.Key, e.Value, &hint)
	}
}

func (n *listNode) mergeToSkl(skl *skl.Skiplist) {
	next := (*listNode)(atomic.LoadPointer(&n.next))
	if next != nil {
		next.mergeToSkl(skl)
	}
	atomic.StorePointer(&n.next, nil)
	n.putToSkl(skl, n.entries)
}

func (n *listNode) get(key []byte) (y.ValueStruct, bool) {
	i := sort.Search(len(n.entries), func(i int) bool {
		return y.CompareKeysWithVer(n.entries[i].Key, key) >= 0
	})
	if i < len(n.entries) && y.SameKey(key, n.entries[i].Key) {
		return n.entries[i].Value, true
	}
	return y.ValueStruct{}, false
}

func (n *listNode) newIterator(reverse bool) *listNodeIterator {
	return &listNodeIterator{reversed: reverse, n: n}
}

type listNodeIterator struct {
	idx      int
	n        *listNode
	reversed bool
}

func (it *listNodeIterator) Next() {
	if !it.reversed {
		it.idx++
	} else {
		it.idx--
	}
}

func (it *listNodeIterator) Rewind() {
	if !it.reversed {
		it.idx = 0
	} else {
		it.idx = len(it.n.entries) - 1
	}
}

func (it *listNodeIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.n.entries), func(i int) bool {
		return y.CompareKeysWithVer(it.n.entries[i].Key, key) >= 0
	})
	if it.reversed {
		if !it.Valid() || !bytes.Equal(it.Key(), key) {
			it.idx--
		}
	}
}

func (it *listNodeIterator) Key() []byte { return it.n.entries[it.idx].Key }

func (it *listNodeIterator) Value() y.ValueStruct { return it.n.entries[it.idx].Value }

func (it *listNodeIterator) FillValue(vs *y.ValueStruct) { *vs = it.Value() }

func (it *listNodeIterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.n.entries) }
