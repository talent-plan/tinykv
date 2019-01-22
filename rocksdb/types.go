//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

import "encoding/binary"

type ValueType uint8

const (
	TypeDeletion ValueType = iota
	TypeValue
	TypeMerge
)

func (vt ValueType) IsValue() bool {
	return vt <= TypeMerge
}

type Comparator func(key1 []byte, key2 []byte) int

// CompareInternalKey compares two keys order by:
//    increasing user key (according to user-supplied comparator)
//    decreasing sequence number
//    decreasing type (though sequence# should be enough to disambiguate)
func (c Comparator) CompareInternalKey(key1, key2 []byte) int {
	k1 := key1[:len(key1)-8]
	k2 := key2[:len(key2)-8]
	cmp := c(k1, k2)
	if cmp == 0 {
		num1 := rocksEndian.Uint64(key1[len(key1)-8:])
		num2 := rocksEndian.Uint64(key2[len(key2)-8:])
		if num1 > num2 {
			cmp = -1
		} else if num1 < num2 {
			cmp = +1
		}
	}
	return cmp
}

type TableProperties struct {
	DataSize            uint64
	IndexSize           uint64
	FilterSize          uint64
	RawKeySize          uint64
	RawValueSize        uint64
	NumDataBlocks       uint64
	NumEntries          uint64
	ColumnFamilyID      uint64
	ColumnFamilyName    string
	CompressionName     string
	FilterPolicyName    string
	CreationTime        uint64
	OldestKeyTime       uint64
	PrefixExtractorName string
}

type blockHandle struct {
	Offset uint64
	Size   uint64
}

func (h blockHandle) Encode() []byte {
	result := make([]byte, 2*binary.MaxVarintLen64)
	n := h.EncodeTo(result)
	return result[:n]
}

func (h blockHandle) EncodeTo(buf []byte) int {
	var b [binary.MaxVarintLen64]byte
	cursor := 0
	off := encodeVarint64(buf[:], h.Offset)
	copy(buf[cursor:], off)
	cursor += len(off)
	sz := encodeVarint64(b[:], h.Size)
	copy(buf[cursor:], sz)
	return cursor + len(sz)
}

func (h *blockHandle) Decode(buf []byte) int {
	off, n1 := binary.Uvarint(buf)
	sz, n2 := binary.Uvarint(buf[n1:])
	h.Offset = off
	h.Size = sz
	return n1 + n2
}

type internalKey struct {
	UserKey        []byte
	SequenceNumber uint64
	ValueType      ValueType
}

func (ikey *internalKey) Encode() []byte {
	buf := make([]byte, len(ikey.UserKey)+8)
	copy(buf, ikey.UserKey)
	rocksEndian.PutUint64(buf[len(ikey.UserKey):], ikey.packSeqAndType())
	return buf
}

func (ikey *internalKey) Decode(encoded []byte) {
	ikey.UserKey = ikey.UserKey[:0]
	userKeyLen := len(encoded) - 8
	ikey.UserKey = append(ikey.UserKey, encoded[0:userKeyLen]...)
	ikey.unpackSeqAndType(rocksEndian.Uint64(encoded[userKeyLen:]))
}

func (ikey *internalKey) packSeqAndType() uint64 {
	return ikey.SequenceNumber<<8 | uint64(ikey.ValueType)
}

func (ikey *internalKey) unpackSeqAndType(pack uint64) {
	ikey.ValueType = ValueType(pack & 0xff)
	ikey.SequenceNumber = pack >> 8
}
