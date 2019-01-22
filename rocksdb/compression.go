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
	"math"

	"github.com/DataDog/zstd"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

func snappyCompress(input []byte) []byte {
	outputBound := snappy.MaxEncodedLen(len(input))
	output := make([]byte, outputBound)
	return snappy.Encode(output, input)
}

func lz4Compress(input []byte) []byte {
	rawLen := len(input)
	if rawLen > math.MaxUint32 {
		return nil
	}

	var varintBuf [5]byte
	decompressedSize := encodeVarint32(varintBuf[:], uint32(rawLen))
	outputBound := lz4.CompressBlockBound(rawLen)
	output := make([]byte, len(decompressedSize)+outputBound)
	copy(output, decompressedSize)
	var ht [1 << 16]int
	n, err := lz4.CompressBlock(input, output[len(decompressedSize):], ht[:])
	if err != nil || n == 0 {
		return nil
	}
	return output[:len(decompressedSize)+n]
}

func zstdCompress(input []byte) []byte {
	rawLen := len(input)
	if rawLen > math.MaxUint32 {
		return nil
	}

	var varintBuf [5]byte
	decompressedSize := encodeVarint32(varintBuf[:], uint32(rawLen))
	outputBound := zstd.CompressBound(rawLen)
	output := make([]byte, len(decompressedSize)+outputBound)
	copy(output, decompressedSize)
	compressed, err := zstd.CompressLevel(output[len(decompressedSize):], input, 3)
	if err != nil {
		return nil
	}
	return output[:len(decompressedSize)+len(compressed)]
}

func isGoodCompressionRatio(compressed, raw []byte) bool {
	cl, rl := len(compressed), len(raw)
	return cl < rl-(rl/8)
}

func CompressBlock(raw []byte, tp CompressionType) ([]byte, bool) {
	var compressed []byte
	switch tp {
	case CompressionLz4:
		compressed = lz4Compress(raw)
	case CompressionSnappy:
		compressed = snappyCompress(raw)
	case CompressionZstd:
		compressed = zstdCompress(raw)
	case CompressionNone:
		return raw, false
	}
	if !isGoodCompressionRatio(compressed, raw) {
		return raw, false
	}
	return compressed, true
}
