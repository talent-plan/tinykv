//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

var (
	_ SliceTransform = new(FixedPrefixSliceTransform)
	_ SliceTransform = new(FixedSuffixSliceTransform)
	_ SliceTransform = new(NoopSliceTransform)
)

type SliceTransform interface {
	Transform([]byte) []byte
	InDomain([]byte) bool
	InRange([]byte) bool
}

type FixedPrefixSliceTransform struct {
	prefixLen int
}

func NewFixedPrefixSliceTransform(prefixLen int) *FixedPrefixSliceTransform {
	return &FixedPrefixSliceTransform{prefixLen: prefixLen}
}

func (st *FixedPrefixSliceTransform) Transform(key []byte) []byte {
	return key[:st.prefixLen]
}

func (st *FixedPrefixSliceTransform) InDomain(key []byte) bool {
	return len(key) >= st.prefixLen
}

func (st *FixedPrefixSliceTransform) InRange(key []byte) bool {
	return true
}

type FixedSuffixSliceTransform struct {
	suffixLen int
}

func NewFixedSuffixSliceTransform(suffixLen int) *FixedSuffixSliceTransform {
	return &FixedSuffixSliceTransform{suffixLen: suffixLen}
}

func (st *FixedSuffixSliceTransform) Transform(key []byte) []byte {
	mid := len(key) - st.suffixLen
	return key[:mid]
}

func (st *FixedSuffixSliceTransform) InDomain(key []byte) bool {
	return len(key) >= st.suffixLen
}

func (st *FixedSuffixSliceTransform) InRange(key []byte) bool {
	return true
}

type NoopSliceTransform struct{}

func NewNoopSliceTransform() *NoopSliceTransform {
	return &NoopSliceTransform{}
}

func (st *NoopSliceTransform) Transform(key []byte) []byte {
	return key
}

func (st *NoopSliceTransform) InDomain(key []byte) bool {
	return true
}

func (st *NoopSliceTransform) InRange(key []byte) bool {
	return true
}
