//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

import "github.com/coocood/badger/y"

// 1-byte type + 32-bit crc
const blockTrailerSize = 5

// blockBuilder port from RocksDB's blockBuilder without Hash Index Support
type blockBuilder struct {
	restartInterval int
	counter         int
	estimate        int
	restarts        []uint32
	buf             []byte
	lastKey         []byte
}

func newBlockBuilder(restartInterval int) *blockBuilder {
	b := new(blockBuilder)
	b.Init(restartInterval)
	return b
}

func (b *blockBuilder) Init(restartInterval int) {
	b.restartInterval = restartInterval
	b.restarts = []uint32{0}
}

func (b *blockBuilder) Reset() {
	b.counter = 0
	b.estimate = 0
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:1]
	b.lastKey = b.lastKey[:0]
}

func (b *blockBuilder) Add(key, value []byte) {
	y.Assert(b.counter <= b.restartInterval)
	var prefixLen uint32
	if b.counter >= b.restartInterval {
		// Restart compression
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
		b.lastKey = y.SafeCopy(b.lastKey, key)
	} else {
		prefixLen = uint32(differenceOffset(key, b.lastKey))
		b.lastKey = y.SafeCopy(b.lastKey, key)
	}

	currSz := len(b.buf)

	b.buf = appendVarint32(b.buf, prefixLen)
	b.buf = appendVarint32(b.buf, uint32(len(key))-prefixLen)
	b.buf = appendVarint32(b.buf, uint32(len(value)))

	b.buf = append(b.buf, key[prefixLen:]...)
	b.buf = append(b.buf, value...)

	b.counter++
	b.estimate += len(b.buf) - currSz
}

func (b *blockBuilder) Empty() bool {
	return len(b.buf) == 0
}

func (b *blockBuilder) Finish() []byte {
	var encodeBuf [4]byte
	for _, restart := range b.restarts {
		rocksEndian.PutUint32(encodeBuf[:], restart)
		b.buf = append(b.buf, encodeBuf[:]...)
	}
	rocksEndian.PutUint32(encodeBuf[:], uint32(len(b.restarts)))
	return append(b.buf, encodeBuf[:]...)
}

func (b *blockBuilder) EstimateSize() int {
	return b.estimate
}

func (b *blockBuilder) EstimateSizeAfterKV(key, value []byte) int {
	estimate := b.EstimateSize()
	estimate += len(key) + len(value)

	if b.counter >= b.restartInterval {
		// a new restart entry
		estimate += 4
	}

	// Note: this is an imprecise estimate as we will have to encoded size, one
	// for shared key and one for non-shared key.
	estimate += 4 + 4 + 4

	return estimate
}
