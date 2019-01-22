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
	"encoding/binary"
	"math"
	"sort"
)

const (
	propColumnFamilyId      = "rocksdb.column.family.id"
	propCompression         = "rocksdb.compression"
	propCreationTime        = "rocksdb.creation.time"
	propDataSize            = "rocksdb.data.size"
	propFilterPolicy        = "rocksdb.filter.policy"
	propFilterSize          = "rocksdb.filter.size"
	propFixedKeyLength      = "rocksdb.fixed.key.length"
	propFormatVersion       = "rocksdb.format.version"
	propIndexKeyIsUserKey   = "rocksdb.index.key.is.user.key"
	propIndexSize           = "rocksdb.index.size"
	propNumDataBlocks       = "rocksdb.num.data.blocks"
	propNumEntries          = "rocksdb.num.entries"
	propOldestKeyTime       = "rocksdb.oldest.key.time"
	propPrefixExtractorName = "rocksdb.prefix.extractor.name"
	propRawKeySize          = "rocksdb.raw.key.size"
	propRawValueSize        = "rocksdb.raw.value.size"
)

type PropsInjector func(*PropsBlockBuilder)

type PropsBlockBuilder struct {
	blockBuilder blockBuilder
	props        []propKV
}

type propKV struct {
	key   []byte
	value []byte
}

func newPropsBlockBuilder() *PropsBlockBuilder {
	b := new(PropsBlockBuilder)
	b.blockBuilder.Init(math.MaxInt32)
	return b
}

func (b *PropsBlockBuilder) Add(name string, value []byte) {
	b.props = append(b.props, propKV{key: []byte(name), value: value})
}

func (b *PropsBlockBuilder) AddUint64(name string, value uint64) {
	var buf [binary.MaxVarintLen64]byte
	b.Add(name, encodeVarint64(buf[:], value))
}

func (b *PropsBlockBuilder) AddString(name, value string) {
	b.Add(name, []byte(value))
}

func (b *PropsBlockBuilder) Finish() []byte {
	sort.Slice(b.props, func(i, j int) bool {
		return bytes.Compare(b.props[i].key, b.props[j].key) < 0
	})
	for _, p := range b.props {
		b.blockBuilder.Add(p.key, p.value)
	}
	return b.blockBuilder.Finish()
}
