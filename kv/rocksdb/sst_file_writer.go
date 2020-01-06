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
	"os"

	"github.com/coocood/badger/y"
	"github.com/pingcap/errors"
)

var (
	ErrKeyOrder       = errors.New("Keys must be added in order")
	ErrNotSupportType = errors.New("Value type is not supported")
)

const (
	propExternalSstFileVersion = "rocksdb.external_sst_file.version"
	propGlobalSeqNo            = "rocksdb.external_sst_file.global_seqno"
)

type SstFileWriter struct {
	file       *os.File
	builder    *BlockBasedTableBuilder
	lastKey    []byte
	comparator Comparator
}

func NewSstFileWriter(f *os.File, opts *BlockBasedTableOptions) *SstFileWriter {
	w := new(SstFileWriter)
	opts.PropsInjectors = append(opts.PropsInjectors, func(builder *PropsBlockBuilder) {
		builder.AddUint64(propExternalSstFileVersion, 2)
		builder.AddUint64(propGlobalSeqNo, 0)
	})
	w.file = f
	w.builder = NewBlockBasedTableBuilder(f, opts)
	w.comparator = opts.Comparator
	return w
}

func (w *SstFileWriter) Put(key, value []byte) error {
	return w.add(key, value, TypeValue)
}

func (w *SstFileWriter) Merge(key, value []byte) error {
	return w.add(key, value, TypeMerge)
}

func (w *SstFileWriter) Delete(key []byte) error {
	return w.add(key, nil, TypeDeletion)
}

func (w *SstFileWriter) Finish() error {
	return w.builder.Finish()
}

func (w *SstFileWriter) add(key, value []byte, tp ValueType) error {
	if !tp.IsValue() {
		return ErrNotSupportType
	}
	if w.lastKey != nil {
		if w.comparator(key, w.lastKey) <= 0 {
			return ErrKeyOrder
		}
	}

	ikey := InternalKey{
		UserKey:        key,
		SequenceNumber: 0,
		ValueType:      tp,
	}
	if err := w.builder.Add(ikey.Encode(), value); err != nil {
		return err
	}

	w.lastKey = y.SafeCopy(w.lastKey, key)

	return nil
}

func (w *SstFileWriter) Close() error {
	return w.file.Close()
}
