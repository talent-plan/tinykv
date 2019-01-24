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

	"github.com/pierrec/lz4"
	"github.com/pingcap/errors"
)

var ErrDecompress = errors.New("Error during decompress")

func lz4Compress(input, dst []byte) []byte {
	rawLen := len(input)
	if rawLen > math.MaxUint32 {
		return nil
	}

	var varintBuf [5]byte
	decompressedSize := encodeVarint32(varintBuf[:], uint32(rawLen))
	outputBound := lz4.CompressBlockBound(rawLen)
	size := len(decompressedSize) + outputBound
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	copy(dst, decompressedSize)
	var ht [1 << 16]int
	n, err := lz4.CompressBlock(input, dst[len(decompressedSize):], ht[:])
	if err != nil || n == 0 {
		return nil
	}
	return dst[:len(decompressedSize)+n]
}

func isGoodCompressionRatio(compressed, input []byte) bool {
	cl, rl := len(compressed), len(input)
	return cl < rl-(rl/8)
}

func CompressBlock(tp CompressionType, input, dst []byte) ([]byte, bool) {
	var compressed []byte
	switch tp {
	case CompressionLz4:
		compressed = lz4Compress(input, dst)
	case CompressionNone:
		return input, false
	case CompressionSnappy:
		panic("unsupported")
	case CompressionZstd:
		panic("unsupported")
	}
	if compressed == nil || !isGoodCompressionRatio(compressed, input) {
		return input, false
	}
	return compressed, true
}

func lz4Decompress(input, dst []byte) ([]byte, error) {
	size, n := decodeVarint32(input)
	if n <= 0 {
		return input, ErrDecompress
	}

	if uint32(cap(dst)) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	_, err := lz4.UncompressBlock(input[n:], dst)
	return dst, err
}

func DecompressBlock(tp CompressionType, input, dst []byte) ([]byte, error) {
	switch tp {
	case CompressionLz4:
		return lz4Decompress(input, dst)
	case CompressionNone:
		return input, nil
	case CompressionSnappy:
		panic("unsupported")
	case CompressionZstd:
		panic("unsupported")
	default:
		panic("unreachable branch")
	}
}
