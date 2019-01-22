//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

import (
	"bytes"
	"github.com/coocood/badger/y"
)

type fullFilterBlockBuilder struct {
	prefixExtractor      SliceTransform
	bitsBuilder          fullFilterBitsBuilder
	wholeKeyFiltering    bool
	lastWholeKeyRecorded bool
	lastWholeKey         []byte
	lastPrefixRecorded   bool
	lastPrefix           []byte
	numAdded             int
}

func newFullFilterBlockBuilder(opts *BlockBasedTableOptions) *fullFilterBlockBuilder {
	return &fullFilterBlockBuilder{
		prefixExtractor:   opts.PrefixExtractor,
		wholeKeyFiltering: opts.WholeKeyFiltering,
		bitsBuilder: fullFilterBitsBuilder{
			bitsPerKey: opts.BloomBitsPerKey,
			numProbes:  opts.BloomNumProbes,
		},
	}
}

func (b *fullFilterBlockBuilder) Add(key []byte) {
	addPrefix := b.prefixExtractor != nil && b.prefixExtractor.InDomain(key)
	if b.wholeKeyFiltering {
		if !addPrefix {
			b.addKey(key)
		} else {
			// if both whole_key and prefix are added to bloom then we will have whole
			// key and prefix addition being interleaved and thus cannot rely on the
			// bits builder to properly detect the duplicates by comparing with the
			// last item.
			if !b.lastWholeKeyRecorded || bytes.Compare(b.lastWholeKey, key) != 0 {
				b.addKey(key)
				b.lastWholeKeyRecorded = true
				b.lastWholeKey = y.SafeCopy(b.lastWholeKey, key)
			}
		}
	}
	if addPrefix {
		b.addPrefix(key)
	}
}

func (b *fullFilterBlockBuilder) Empty() bool {
	return b.numAdded == 0
}

func (b *fullFilterBlockBuilder) addKey(key []byte) {
	b.bitsBuilder.AddKey(key)
	b.numAdded++
}

func (b *fullFilterBlockBuilder) addPrefix(key []byte) {
	prefix := b.prefixExtractor.Transform(key)

	if b.wholeKeyFiltering {
		if !b.lastWholeKeyRecorded || bytes.Compare(b.lastPrefix, prefix) != 0 {
			b.addKey(prefix)
			b.lastPrefixRecorded = true
			b.lastPrefix = y.SafeCopy(b.lastPrefix, prefix)
		}
		return
	}

	b.addKey(prefix)
}

func (b *fullFilterBlockBuilder) Finish() []byte {
	if b.numAdded != 0 {
		b.numAdded = 0
		return b.bitsBuilder.Finish()
	}
	return nil
}

type fullFilterBitsBuilder struct {
	bitsPerKey  int
	numProbes   int
	hashEntries []uint32
}

func (b *fullFilterBitsBuilder) AddKey(key []byte) {
	hash := bloomHash(key)
	if len(b.hashEntries) == 0 || hash != b.hashEntries[len(b.hashEntries)-1] {
		b.hashEntries = append(b.hashEntries, hash)
	}
}

func (b *fullFilterBitsBuilder) Finish() []byte {
	buf, totalBits, numLines := b.prepareFilterBuffer()
	if totalBits != 0 && numLines != 0 {
		for _, h := range b.hashEntries {
			b.addHash(h, buf, totalBits, numLines)
		}
	}
	buf[totalBits/8] = byte(b.numProbes)
	rocksEndian.PutUint32(buf[totalBits/8+1:], numLines)
	return buf
}

func (b *fullFilterBitsBuilder) prepareFilterBuffer() ([]byte, uint32, uint32) {
	var numLines, totalBits uint32
	if len(b.hashEntries) != 0 {
		numLines, totalBits = b.optimizeBitsForLocality(uint32(len(b.hashEntries) * b.bitsPerKey))
	}

	sz := totalBits/8 + 5
	return make([]byte, sz), totalBits, numLines
}

func (b *fullFilterBitsBuilder) optimizeBitsForLocality(bits uint32) (uint32, uint32) {
	numLines := (bits + cacheLineSize*8 - 1) / (cacheLineSize * 8)
	if numLines%2 == 0 {
		numLines++
	}
	return numLines, numLines * (cacheLineSize * 8)
}

func (b *fullFilterBitsBuilder) addHash(hash uint32, buf []byte, totalBits, numLines uint32) {
	delta := (hash >> 17) | (hash << 15)
	base := (hash % numLines) * (cacheLineSize * 8)
	for i := 0; i < b.numProbes; i++ {
		bitpos := base + (hash % (cacheLineSize * 8))
		buf[bitpos/8] |= 1 << (bitpos % 8)
		hash += delta
	}
}

func bloomHash(key []byte) uint32 {
	return rocksHash(key, 0xbc9f1d34)
}
