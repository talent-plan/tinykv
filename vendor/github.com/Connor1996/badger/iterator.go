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
	"fmt"
	"math"
	"sort"
	"sync/atomic"

	"github.com/Connor1996/badger/table"
	"github.com/Connor1996/badger/y"
	"github.com/dgryski/go-farm"
)

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	err      error
	db       *DB
	key      []byte
	vptr     []byte
	meta     byte // We need to store meta to know about bitValuePointer.
	userMeta []byte
	slice    *y.Slice
	next     *Item
	version  uint64
	txn      *Txn
}

// String returns a string representation of Item
func (item *Item) String() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)
}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please use KeyCopy
func (item *Item) Key() []byte {
	return item.key
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.version
}

// IsEmpty checks if the value is empty.
func (item *Item) IsEmpty() bool {
	return len(item.vptr) == 0
}

// Value retrieves the value of the item from the value log.
//
// This method must be called within a transaction. Calling it outside a
// transaction is considered undefined behavior. If an iterator is being used,
// then Item.Value() is defined in the current iteration only, because items are
// reused.
//
// If you need to use a value outside a transaction, please use Item.ValueCopy
// instead, or copy it yourself. Value might change once discard or commit is called.
// Use ValueCopy if you want to do a Set after Get.
func (item *Item) Value() ([]byte, error) {
	if item.meta&bitValuePointer > 0 {
		if item.slice == nil {
			item.slice = new(y.Slice)
		}
		if item.txn.blobCache == nil {
			item.txn.blobCache = map[uint32]*blobCache{}
		}
		return item.db.blobManger.read(item.vptr, item.slice, item.txn.blobCache)
	}
	return item.vptr, nil
}

// ValueSize returns the size of the value without the cost of retrieving the value.
func (item *Item) ValueSize() int {
	if item.meta&bitValuePointer > 0 {
		var bp blobPointer
		bp.decode(item.vptr)
		return int(bp.length)
	}
	return len(item.vptr)
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned. Tip: It might make sense to reuse the returned slice as dst argument for the next call.
//
// This function is useful in long running iterate/update transactions to avoid a write deadlock.
// See Github issue: https://github.com/Connor1996/badger/issues/315
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	buf, err := item.Value()
	if err != nil {
		return nil, err
	}
	return y.SafeCopy(dst, buf), nil
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	return true
}

// IsDeleted returns true if item contains deleted or expired value.
func (item *Item) IsDeleted() bool {
	return isDeleted(item.meta)
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	return int64(len(item.key) + len(item.vptr))
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() []byte {
	return item.userMeta
}

// IteratorOptions is used to set options when iterating over Badger key-value
// stores.
//
// This package provides DefaultIteratorOptions which contains options that
// should work for most applications. Consider using that as a starting point
// before customizing it for your own needs.
type IteratorOptions struct {
	Reverse     bool // Direction of iteration. False is forward, true is backward.
	AllVersions bool // Fetch all valid versions of the same key.

	// StartKey and EndKey are used to prune non-overlapping table iterators.
	// They are not boundary limits.
	StartKey       []byte
	startKeyWithTS []byte
	EndKey         []byte
	endKeyWithTS   []byte

	internalAccess bool // Used to allow internal access to badger keys.
}

func (opts *IteratorOptions) hasRange() bool {
	return len(opts.startKeyWithTS) > 0 && len(opts.endKeyWithTS) > 0
}

func (opts *IteratorOptions) OverlapPending(it *pendingWritesIterator) bool {
	if it == nil {
		return false
	}
	if !opts.hasRange() {
		return true
	}
	if y.CompareKeysWithVer(opts.endKeyWithTS, it.entries[0].Key) <= 0 {
		return false
	}
	if y.CompareKeysWithVer(opts.startKeyWithTS, it.entries[len(it.entries)-1].Key) > 0 {
		return false
	}
	return true
}

func (opts *IteratorOptions) OverlapMemTable(t *table.MemTable) bool {
	if t.Empty() {
		return false
	}
	if !opts.hasRange() {
		return true
	}
	iter := t.NewIterator(false)
	iter.Seek(opts.startKeyWithTS)
	if !iter.Valid() {
		return false
	}
	if y.CompareKeysWithVer(iter.Key(), opts.endKeyWithTS) >= 0 {
		return false
	}
	return true
}

func (opts *IteratorOptions) OverlapTable(t *table.Table) bool {
	if !opts.hasRange() {
		return true
	}
	return t.HasOverlap(opts.startKeyWithTS, opts.endKeyWithTS, false)
}

func (opts *IteratorOptions) OverlapTables(tables []*table.Table) []*table.Table {
	if len(tables) == 0 {
		return nil
	}
	if !opts.hasRange() {
		return tables
	}
	startIdx := sort.Search(len(tables), func(i int) bool {
		t := tables[i]
		return y.CompareKeysWithVer(opts.startKeyWithTS, t.Biggest()) <= 0
	})
	if startIdx == len(tables) {
		return nil
	}
	tables = tables[startIdx:]
	endIdx := sort.Search(len(tables), func(i int) bool {
		t := tables[i]
		return y.CompareKeysWithVer(t.Smallest(), opts.endKeyWithTS) >= 0
	})
	tables = tables[:endIdx]
	overlapTables := make([]*table.Table, 0, 8)
	for _, t := range tables {
		if opts.OverlapTable(t) {
			overlapTables = append(overlapTables, t)
		}
	}
	return overlapTables
}

// DefaultIteratorOptions contains default options when iterating over Badger key-value stores.
var DefaultIteratorOptions = IteratorOptions{
	Reverse:     false,
	AllVersions: false,
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	iitr   y.Iterator
	txn    *Txn
	readTs uint64

	opt   IteratorOptions
	item  *Item
	itBuf Item
	vs    y.ValueStruct

	lastKey []byte // Used to skip over multiple versions of the same key.
}

// NewIterator returns a new iterator. Depending upon the options, either only keys, or both
// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
// Avoid long running iterations in update transactions.
func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	atomic.AddInt32(&txn.numIterators, 1)

	tables := txn.db.getMemTables()
	if len(opt.StartKey) > 0 {
		opt.startKeyWithTS = y.KeyWithTs(opt.StartKey, math.MaxUint64)
	}
	if len(opt.EndKey) > 0 {
		opt.endKeyWithTS = y.KeyWithTs(opt.EndKey, math.MaxUint64)
	}
	var iters []y.Iterator
	if itr := txn.newPendingWritesIterator(opt.Reverse); opt.OverlapPending(itr) {
		iters = append(iters, itr)
	}
	for i := 0; i < len(tables); i++ {
		if opt.OverlapMemTable(tables[i]) {
			iters = append(iters, tables[i].NewIterator(opt.Reverse))
		}
	}
	iters = txn.db.lc.appendIterators(iters, opt) // This will increment references.
	res := &Iterator{
		txn:    txn,
		iitr:   table.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
	}
	res.itBuf.db = txn.db
	res.itBuf.txn = txn
	res.itBuf.slice = new(y.Slice)
	return res
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *Iterator) Item() *Item {
	tx := it.txn
	if tx.update {
		// Track reads if this is an update txn.
		tx.reads = append(tx.reads, farm.Fingerprint64(it.item.Key()))
	}
	return it.item
}

// Valid returns false when iteration is done.
func (it *Iterator) Valid() bool { return it.item != nil }

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.item != nil && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	atomic.AddInt32(&it.txn.numIterators, -1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *Iterator) Next() {
	if !it.opt.Reverse {
		it.iitr.Next()
		it.parseItemForward()
		return
	}
	it.parseItemReverse()
	return
}

func (it *Iterator) parseItemForward() {
	iitr := it.iitr
	for iitr.Valid() {
		keyWithTS := iitr.Key()
		if !it.opt.internalAccess && keyWithTS[0] == '!' {
			iitr.Next()
			continue
		}
		version := y.ParseTs(keyWithTS)
		if version > it.readTs {
			iitr.Next()
			continue
		}
		key := y.ParseKey(keyWithTS)
		if !it.opt.AllVersions {
			if possibleSameKey(it.lastKey, key) && bytes.Equal(it.lastKey, key) {
				iitr.Next()
				continue
			}
			it.lastKey = y.SafeCopy(it.lastKey, key)
			iitr.FillValue(&it.vs)
			if isDeleted(it.vs.Meta) {
				iitr.Next()
				continue
			}
		} else {
			iitr.FillValue(&it.vs)
		}
		item := &it.itBuf
		item.version = version
		item.key = key
		item.meta = it.vs.Meta
		item.userMeta = it.vs.UserMeta
		item.vptr = it.vs.Value
		it.item = item
		return
	}
	it.item = nil
}

func possibleSameKey(aKey, bKey []byte) bool {
	if len(aKey) != len(bKey) {
		return false
	}
	lastIdx := len(aKey) - 1
	if aKey[lastIdx] != bKey[lastIdx] {
		return false
	}
	return true
}

func isDeleted(meta byte) bool {
	return meta&bitDelete > 0
}

func (it *Iterator) setItem(item *Item) {
	it.item = item
}

// parseItemReverseOnce handles reverse iteration
// implementation. We store keys such that their versions are sorted in descending order. This makes
// forward iteration efficient, but reverse iteration complicated. This tradeoff is better because
// forward iteration is more common than reverse.
//
// This function advances the iterator.
func (it *Iterator) parseItemReverseOnce() bool {
	mi := it.iitr
	key := mi.Key()

	// Skip badger keys.
	if !it.opt.internalAccess && key[0] == '!' {
		mi.Next()
		return false
	}

	// Skip any versions which are beyond the readTs.
	version := y.ParseTs(key)
	if version > it.readTs {
		mi.Next()
		return false
	}

	if it.opt.AllVersions {
		// Return deleted or expired values also, otherwise user can't figure out
		// whether the key was deleted.
		it.iitr.FillValue(&it.vs)
		item := &it.itBuf
		it.fill(item)
		it.setItem(item)
		mi.Next()
		return true
	}

FILL:
	// If deleted, advance and return.
	mi.FillValue(&it.vs)
	if isDeleted(it.vs.Meta) {
		mi.Next()
		return false
	}

	item := &it.itBuf
	it.fill(item)
	// fill item based on current cursor position. All Next calls have returned, so reaching here
	// means no Next was called.

	mi.Next() // Advance but no fill item yet.
	if !mi.Valid() {
		it.setItem(item)
		return true
	}

	// Reverse direction.
	nextTs := y.ParseTs(mi.Key())
	mik := y.ParseKey(mi.Key())
	if nextTs <= it.readTs && bytes.Equal(mik, item.key) {
		// This is a valid potential candidate.
		goto FILL
	}
	// Ignore the next candidate. Return the current one.
	it.setItem(item)
	return true
}

func (it *Iterator) fill(item *Item) {
	item.meta = it.vs.Meta
	item.userMeta = it.vs.UserMeta

	key := it.iitr.Key()
	item.version = y.ParseTs(key)
	item.key = y.SafeCopy(item.key, y.ParseKey(key))
	item.vptr = y.SafeCopy(item.vptr, it.vs.Value)
}

func (it *Iterator) parseItemReverse() {
	it.item = nil
	for it.iitr.Valid() {
		if it.parseItemReverseOnce() {
			// parseItemReverseOnce calls one extra next.
			// This is used to deal with the complexity of reverse iteration.
			break
		}
	}
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided if iterating in the forward direction. Behavior would be reversed is
// iterating backwards.
func (it *Iterator) Seek(key []byte) {
	it.lastKey = it.lastKey[:0]
	if !it.opt.Reverse {
		key = y.KeyWithTs(key, it.txn.readTs)
		it.iitr.Seek(key)
		it.parseItemForward()
		return
	}

	if len(key) == 0 {
		it.iitr.Rewind()
		it.parseItemReverse()
		return
	}
	key = y.KeyWithTs(key, 0)
	it.iitr.Seek(key)
	it.parseItemReverse()
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *Iterator) Rewind() {
	it.lastKey = it.lastKey[:0]
	if !it.opt.Reverse {
		it.iitr.Rewind()
		it.parseItemForward()
		return
	}
	it.iitr.Rewind()
	it.parseItemReverse()
}
