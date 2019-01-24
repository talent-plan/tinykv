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
	"encoding/binary"
	"hash"
	"hash/crc32"
)

var (
	rocksEndian   = binary.LittleEndian
	rocksCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

const cacheLineSize = 64

func differenceOffset(lhs, rhs []byte) int {
	l := len(lhs)
	if len(rhs) < len(lhs) {
		l = len(rhs)
	}

	off := 0
	for ; off < l; off++ {
		if lhs[off] != rhs[off] {
			break
		}
	}
	return off
}

func encodeVarint32(buf []byte, v uint32) []byte {
	_ = buf[4]
	const B = 128
	if v < (1 << 7) {
		buf[0] = byte(v)
		return buf[:1]
	} else if v < (1 << 14) {
		buf[0] = byte(v | B)
		buf[1] = byte(v >> 7)
		return buf[:2]
	} else if v < (1 << 21) {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[2] = byte(v >> 14)
		return buf[:3]
	} else if v < (1 << 28) {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[2] = byte((v >> 14) | B)
		buf[3] = byte(v >> 21)
		return buf[:4]
	} else {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[2] = byte((v >> 14) | B)
		buf[3] = byte((v >> 21) | B)
		buf[4] = byte(v >> 28)
		return buf[:5]
	}
}

func decodeVarint32(buf []byte) (uint32, int) {
	result := buf[0]
	if (result & 128) == 0 {
		return uint32(result), 1
	}
	return decodeVarint32Slow(buf)
}

func decodeVarint32Slow(buf []byte) (uint32, int) {
	var result, shift uint32
	var i int
	for shift <= 28 && i < len(buf) {
		b := buf[i]
		i += 1
		if b&128 != 0 {
			result |= uint32(b&127) << shift
		} else {
			result |= uint32(b) << shift
			return result, i
		}

		shift += 7
	}
	return 0, 0
}

func encodeVarint64(buf []byte, v uint64) []byte {
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

func decodeVarint64(buf []byte) (uint64, int) {
	return binary.Uvarint(buf)
}

func appendVarint32(buf []byte, v uint32) []byte {
	var e [5]byte
	result := encodeVarint32(e[:], v)
	return append(buf, result...)
}

const crc32MaskDelta = 0xa282ead8

func maskCrc32(sum uint32) uint32 {
	return ((sum >> 15) | (sum << 17)) + crc32MaskDelta
}

func unmaskCrc32(sum uint32) uint32 {
	rot := sum - crc32MaskDelta
	return (rot >> 17) | (rot << 15)
}

func newCrc32() hash.Hash32 {
	return crc32.New(rocksCrcTable)
}

func extractUserKey(key []byte) []byte {
	return key[:len(key)-8]
}

func rocksHash(data []byte, seed uint32) uint32 {
	const m = 0xc6a4a793
	const r = 24
	h := seed ^ uint32(len(data)*m)

	pos := 0
	for ; pos+4 < len(data); pos += 4 {
		w := rocksEndian.Uint32(data[pos : pos+4])
		h += w
		h *= m
		h ^= h >> 16
	}

	// Pick up remaining bytes
	remain := len(data) - pos
	if remain == 3 {
		h += uint32(int8(data[2])) << 16
	}
	if remain >= 2 {
		h += uint32(int8(data[1])) << 8
	}
	if remain >= 1 {
		h += uint32(int8(data[0]))
		h *= m
		h ^= h >> r
	}
	return h
}
