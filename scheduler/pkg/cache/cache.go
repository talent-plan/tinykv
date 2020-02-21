// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

// Item is the cache entry.
type Item struct {
	Key   uint64
	Value interface{}
}

// Cache is an interface for cache system
type Cache interface {
	// Put puts an item into cache.
	Put(key uint64, value interface{})
	// Get retrives an item from cache.
	Get(key uint64) (interface{}, bool)
	// Peek reads an item from cache. The action is no considered 'Use'.
	Peek(key uint64) (interface{}, bool)
	// Remove eliminates an item from cache.
	Remove(key uint64)
	// Elems return all items in cache.
	Elems() []*Item
	// Len returns current cache size
	Len() int
}
