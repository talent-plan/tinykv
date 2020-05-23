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
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Connor1996/badger/epoch"
	"github.com/Connor1996/badger/y"
	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
)

type oracle struct {
	// curRead must be at the top for memory alignment. See issue #311.
	curRead   uint64 // Managed by the mutex.
	refCount  int64
	isManaged bool // Does not change value, so no locking required.

	sync.Mutex
	writeLock  sync.Mutex
	nextCommit uint64

	// commits stores a key fingerprint and latest commit counter for it.
	// refCount is used to clear out commits map to avoid a memory blowup.
	commits map[uint64]uint64
}

func (o *oracle) addRef() {
	atomic.AddInt64(&o.refCount, 1)
}

func (o *oracle) decrRef() {
	if count := atomic.AddInt64(&o.refCount, -1); count == 0 {
		// Clear out commits maps to release memory.
		o.Lock()
		// Avoids the race where something new is added to commitsMap
		// after we check refCount and before we take Lock.
		if atomic.LoadInt64(&o.refCount) != 0 {
			o.Unlock()
			return
		}
		if len(o.commits) >= 1000 { // If the map is still small, let it slide.
			o.commits = make(map[uint64]uint64)
		}
		o.Unlock()
	}
}

func (o *oracle) readTs() uint64 {
	if o.isManaged {
		return math.MaxUint64
	}
	return atomic.LoadUint64(&o.curRead)
}

func (o *oracle) commitTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextCommit
}

// hasConflict must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, ro := range txn.reads {
		if ts, has := o.commits[ro]; has && ts > txn.readTs {
			return true
		}
	}
	return false
}

func (o *oracle) newCommitTs(txn *Txn) uint64 {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0
	}

	var ts uint64
	if !o.isManaged {
		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextCommit
		o.nextCommit++

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	for _, w := range txn.writes {
		o.commits[w] = ts // Update the commitTs.
	}
	return ts
}

func (o *oracle) allocTs() uint64 {
	o.Lock()
	ts := o.nextCommit
	o.nextCommit++
	o.Unlock()
	return ts
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}

	for {
		curRead := atomic.LoadUint64(&o.curRead)
		if cts <= curRead {
			return
		}
		atomic.CompareAndSwapUint64(&o.curRead, curRead, cts)
	}
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64

	update bool     // update is used to conditionally keep track of reads.
	reads  []uint64 // contains fingerprints of keys read.
	writes []uint64 // contains fingerprints of keys written.

	pendingWrites map[string]*Entry // cache stores any writes done by txn.

	db        *DB
	discarded bool
	guard     *epoch.Guard

	size         int64
	count        int64
	numIterators int32
	blobCache    map[uint32]*blobCache
}

type pendingWritesIterator struct {
	entries  []*Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	y.Assert(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() y.ValueStruct {
	y.Assert(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		Value:    entry.Value,
		Meta:     entry.meta,
		UserMeta: entry.UserMeta,
		Version:  pi.readTs,
	}
}

func (pi *pendingWritesIterator) FillValue(vs *y.ValueStruct) {
	entry := pi.entries[pi.nextIdx]
	vs.Value = entry.Value
	vs.Meta = entry.meta
	vs.UserMeta = entry.UserMeta
	vs.Version = pi.readTs
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (txn *Txn) checkSize(e *Entry) error {
	if len(e.UserMeta) > 255 {
		return ErrUserMetaTooLarge
	}
	// Extra bytes for version in key.
	size := int64(e.estimateSize()) + 10
	if size >= txn.db.opt.MaxTableSize {
		return ErrTxnTooBig
	}
	txn.count++
	txn.size += size
	return nil
}

// Set adds a key-value pair to the database.
//
// It will return ErrReadOnlyTxn if update flag was set to false when creating the
// transaction.
func (txn *Txn) Set(key, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return txn.SetEntry(e)
}

// SetWithMeta adds a key-value pair to the database, along with a metadata
// byte. This byte is stored alongside the key, and can be used as an aid to
// interpret the value or store other contextual bits corresponding to the
// key-value pair.
func (txn *Txn) SetWithMeta(key, val []byte, meta byte) error {
	e := &Entry{Key: key, Value: val, UserMeta: []byte{meta}}
	return txn.SetEntry(e)
}

func (txn *Txn) SetWithMetaSlice(key, val, meta []byte) error {
	e := &Entry{Key: key, Value: val, UserMeta: meta}
	return txn.SetEntry(e)
}

func (txn *Txn) modify(e *Entry) error {
	if !txn.update {
		return ErrReadOnlyTxn
	} else if txn.discarded {
		return ErrDiscardedTxn
	} else if len(e.Key) == 0 {
		return ErrEmptyKey
	} else if len(e.Key) > maxKeySize {
		return exceedsMaxKeySizeError(e.Key)
	} else if int64(len(e.Value)) > txn.db.opt.ValueLogFileSize {
		return exceedsMaxValueSizeError(e.Value, txn.db.opt.ValueLogFileSize)
	}
	if err := txn.checkSize(e); err != nil {
		return err
	}

	fp := farm.Fingerprint64(e.Key) // Avoid dealing with byte arrays.
	txn.writes = append(txn.writes, fp)
	txn.pendingWrites[string(e.Key)] = e
	return nil
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct, along
// with other metadata to the database.
func (txn *Txn) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key. This is done by adding a delete marker for the key at commit timestamp.
// Any reads happening before this timestamp would be unaffected. Any reads after this commit would
// see the deletion.
func (txn *Txn) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	item = new(Item)
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
			if isDeleted(e.meta) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from cache.
			item.meta = e.meta
			item.vptr = e.Value
			item.userMeta = e.UserMeta
			item.key = key
			item.version = txn.readTs
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		fp := farm.Fingerprint64(key)
		txn.reads = append(txn.reads, fp)
	}

	seek := y.KeyWithTs(key, txn.readTs)
	vs := txn.db.get(seek)
	if !vs.Valid() {
		return nil, ErrKeyNotFound
	}
	if isDeleted(vs.Meta) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.db = txn.db
	item.vptr = vs.Value
	item.txn = txn
	return item, nil
}

type keyValuePair struct {
	key   []byte
	hash  uint64
	val   y.ValueStruct
	found bool
}

// MultiGet gets items for keys, if not found, the corresponding item will be nil.
// It only supports read-only transaction for simplicity.
func (txn *Txn) MultiGet(keys [][]byte) (items []*Item, err error) {
	if txn.update {
		return nil, errors.New("not supported")
	}
	if txn.discarded {
		return nil, ErrDiscardedTxn
	}
	keyValuePairs := make([]keyValuePair, len(keys))
	for i, key := range keys {
		if len(key) == 0 {
			return nil, ErrEmptyKey
		}
		keyValuePairs[i].hash = farm.Fingerprint64(key)
		keyValuePairs[i].key = y.KeyWithTs(key, txn.readTs)
	}
	txn.db.multiGet(keyValuePairs)
	items = make([]*Item, len(keys))
	for i, pair := range keyValuePairs {
		if pair.found && !isDeleted(pair.val.Meta) {
			items[i] = &Item{
				key:      keys[i],
				version:  pair.val.Version,
				meta:     pair.val.Meta,
				userMeta: pair.val.UserMeta,
				db:       txn.db,
				vptr:     pair.val.Value,
				txn:      txn,
			}
		}
	}
	return items, nil
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&txn.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
	txn.blobCache = nil
	if txn.update {
		txn.db.orc.decrRef()
	}
	txn.guard.Done()
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit() error {
	if txn.commitTs == 0 && txn.db.opt.managedTxns {
		return ErrManagedTxn
	}
	if txn.discarded {
		return ErrDiscardedTxn
	}
	defer txn.Discard()
	if len(txn.writes) == 0 {
		return nil // Nothing to do.
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+1)
	for _, e := range txn.pendingWrites {
		e.meta |= bitTxn
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	state := txn.db.orc
	state.writeLock.Lock()
	commitTs := state.newCommitTs(txn)
	if commitTs == 0 {
		state.writeLock.Unlock()
		return ErrConflict
	}
	for _, e := range entries {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, commitTs)
	}
	e := &Entry{
		Key:   y.KeyWithTs(txnKey, commitTs),
		Value: []byte(strconv.FormatUint(commitTs, 10)),
		meta:  bitFinTxn,
	}
	entries = append(entries, e)

	req, err := txn.db.sendToWriteCh(entries)
	state.writeLock.Unlock()
	if err != nil {
		return err
	}

	req.Wait()
	state.doneCommit(commitTs)

	return nil
}

// NewTransaction creates a new transaction. Badger supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews. Badger achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//  txn := db.NewTransaction(false)
//  defer txn.Discard()
//  // Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	if db.opt.ReadOnly {
		// DB is read-only, force read-only transaction.
		update = false
	}
	readTs := db.orc.readTs()
	txn := &Txn{
		update: update,
		db:     db,
		count:  1,                       // One extra entry for BitFin.
		size:   int64(len(txnKey) + 10), // Some buffer for the extra entry.
		readTs: readTs,
		guard:  db.resourceMgr.AcquireWithPayload(readTs),
	}
	if update {
		txn.pendingWrites = make(map[string]*Entry)
		txn.db.orc.addRef()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.opt.managedTxns {
		return ErrManagedTxn
	}
	txn := db.NewTransaction(false)
	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.opt.managedTxns {
		return ErrManagedTxn
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
