//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

import "golang.org/x/time/rate"

type CompressionType uint8

const (
	CompressionNone   CompressionType = 0x0
	CompressionSnappy                 = 0x1
	CompressionLz4                    = 0x4
	CompressionZstd                   = 0x7
)

func (tp CompressionType) String() string {
	switch tp {
	case CompressionNone:
		return "NoCompression"
	case CompressionSnappy:
		return "Snappy"
	case CompressionLz4:
		return "LZ4"
	case CompressionZstd:
		return "ZSTD"
	default:
		panic("unknown CompressionType")
	}
}

type ChecksumType uint8

const (
	ChecksumNone   ChecksumType = 0x0
	ChecksumCRC32               = 0x1
	ChecksumXXHash              = 0x2
)

type BlockBasedTableOptions struct {
	BlockSize                 int
	BlockSizeDeviation        int
	BlockRestartInterval      int
	IndexBlockRestartInterval int
	BlockAlign                bool
	CompressionType           CompressionType
	ChecksumType              ChecksumType
	EnableIndexCompression    bool
	CreationTime              uint64
	OldestKeyTime             uint64

	PropsInjectors []PropsInjector

	BloomBitsPerKey   int
	BloomNumProbes    int
	WholeKeyFiltering bool

	PrefixExtractorName string
	PrefixExtractor     SliceTransform

	Comparator   Comparator
	BufferSize   int
	BytesPerSync int
	RateLimiter  *rate.Limiter
}

func NewDefaultBlockBasedTableOptions(cmp Comparator) *BlockBasedTableOptions {
	return &BlockBasedTableOptions{
		BlockSize:                 4 * 1024,
		BlockSizeDeviation:        10,
		BlockRestartInterval:      16,
		IndexBlockRestartInterval: 1,
		BlockAlign:                false,
		CompressionType:           CompressionSnappy,
		ChecksumType:              ChecksumCRC32,
		EnableIndexCompression:    true,
		CreationTime:              0,
		OldestKeyTime:             0,

		BloomBitsPerKey:   10,
		BloomNumProbes:    6,
		WholeKeyFiltering: true,

		PrefixExtractorName: "",
		PrefixExtractor:     nil,

		Comparator:   cmp,
		BufferSize:   1 * 1024 * 1024,
		BytesPerSync: 0,
		RateLimiter:  nil,
	}
}
