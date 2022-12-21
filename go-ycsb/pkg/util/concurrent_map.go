package util

import (
	"encoding/json"
	"sync"
)

/*
	This module is modified from github.com/orcaman/concurrent-map.
	MIT license.
*/

// A "thread" safe map of type int:int64.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap struct {
	shards     []*ConcurrentMapShared
	shardCount int
}

// A "thread" safe int to int64 map.
type ConcurrentMapShared struct {
	items        map[int]int64
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// Creates a new concurrent map.
func New(shardCount int) ConcurrentMap {
	m := new(ConcurrentMap)
	m.shardCount = shardCount
	m.shards = make([]*ConcurrentMapShared, shardCount)
	for i := 0; i < shardCount; i++ {
		m.shards[i] = &ConcurrentMapShared{items: make(map[int]int64)}
	}
	return *m
}

// GetShard returns shard under given key
func (m ConcurrentMap) GetShard(key int) *ConcurrentMapShared {
	return m.shards[uint(m.fnv32(key))%uint(m.shardCount)]
}

func (m ConcurrentMap) MSet(data map[int]int64) {
	for key, value := range data {
		m.Set(key, value)
	}
}

// Sets the given value under the specified key.
func (m ConcurrentMap) Set(key int, value int64) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap int64, newValue int64) int64

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMap) Upsert(key int, value int64, cb UpsertCb) (res int64) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap) SetIfAbsent(key int, value int64) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap) Get(key int) (int64, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < m.shardCount; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m ConcurrentMap) Has(key int) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap) Remove(key int) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb func(key int, v int64, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap) RemoveCb(key int, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap) Pop(key int) (v int64, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key int
	Val int64
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot(m ConcurrentMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, m.shardCount)
	wg := sync.WaitGroup{}
	wg.Add(m.shardCount)
	// Foreach shard.
	for index, shard := range m.shards {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[int]int64
func (m ConcurrentMap) Items() map[int]int64 {
	tmp := make(map[int]int64)

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key int, v int64)

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap) IterCb(fn IterCb) {
	for idx := range m.shards {
		shard := m.shards[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []int
func (m ConcurrentMap) Keys() []int {
	count := m.Count()
	ch := make(chan int, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(m.shardCount)
		for _, shard := range m.shards {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]int, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

//Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[int]int64)

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func (m ConcurrentMap) fnv32(key int) uint32 {
	key = (key + 0x7ed55d16) + (key << 12)
	key = (key ^ 0xc761c23c) ^ (key >> 19)
	key = (key + 0x165667b1) + (key << 5)
	key = (key + 0xd3a2646c) ^ (key << 9)
	key = (key + 0xfd7046c5) + (key << 3)
	key = (key ^ 0xb55a4f09) ^ (key >> 16)
	key = key % m.shardCount
	return uint32(key)
}
