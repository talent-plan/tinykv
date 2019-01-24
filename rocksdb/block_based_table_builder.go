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
	"os"

	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/y"
)

const (
	propsBlockHandleKey = "rocksdb.properties"
	bloomBlockHandleKey = "fullfilter.rocksdb.BuiltinBloomFilter"
)

type BlockBasedTableBuilder struct {
	props      TableProperties
	writer     *fileutil.BufferedFileWriter
	comparator Comparator

	dataBlockBuilder  *blockBuilder
	indexBlockBuilder *indexBlockBuilder
	filterBuilder     *fullFilterBlockBuilder

	compressBuf []byte

	offset        uint64
	pendingHandle blockHandle
	lastKey       []byte

	opts *BlockBasedTableOptions

	blockSizeDeviationLimit int
	alignment               int
}

func NewBlockBasedTableBuilder(f *os.File, opts *BlockBasedTableOptions) *BlockBasedTableBuilder {
	w := fileutil.NewBufferedFileWriter(f, opts.BufferSize, opts.BytesPerSync, opts.RateLimiter)
	blockSizeDeviationLimit := ((opts.BlockSize * (100 - opts.BlockSizeDeviation)) + 99) / 100
	alignment := 4 * 1024
	if opts.BlockSize < alignment {
		alignment = opts.BlockSize
	}

	return &BlockBasedTableBuilder{
		writer:                  w,
		comparator:              opts.Comparator,
		dataBlockBuilder:        newBlockBuilder(opts.BlockRestartInterval),
		indexBlockBuilder:       newIndexBlockBuilder(opts.IndexBlockRestartInterval),
		filterBuilder:           newFullFilterBlockBuilder(opts),
		opts:                    opts,
		blockSizeDeviationLimit: blockSizeDeviationLimit,
		alignment:               alignment,
	}
}

func (b *BlockBasedTableBuilder) Add(key, value []byte) error {
	var ikey InternalKey
	ikey.Decode(key)

	// We don't support other record types.
	y.Assert(ikey.ValueType.IsValue())
	if len(b.lastKey) != 0 {
		y.Assert(b.comparator.CompareInternalKey(b.lastKey, key) <= 0)
	}

	if b.shouldFlush(key, value) {
		if err := b.flush(); err != nil {
			return err
		}
		b.indexBlockBuilder.AddIndexEntry(b.lastKey, &b.pendingHandle)
	}

	b.filterBuilder.Add(extractUserKey(key))

	b.dataBlockBuilder.Add(key, value)
	b.props.NumEntries++
	b.props.RawKeySize += uint64(len(key))
	b.props.RawValueSize += uint64(len(value))
	b.lastKey = y.SafeCopy(b.lastKey, key)

	return nil
}

const (
	blockBasedTableMagicNumber = 0x88e241b785f4cff7
	maxBlockHandleLength       = 10 + 10 // two varint64
	footerEncodedLength        = 1 + 2*maxBlockHandleLength + 4 + 8
)

func (b *BlockBasedTableBuilder) Finish() error {
	if err := b.flush(); err != nil {
		return err
	}

	if b.dataBlockBuilder.Empty() {
		b.indexBlockBuilder.AddIndexEntry(b.lastKey, &b.pendingHandle)
	}

	// Write meta blocks and metaindex block with the following order.
	//    1. [meta block: filter]
	//    2. [meta block: index]
	//    3. [meta block: properties]
	//    4. [metaindex block]
	var metaIndexBlockHandle, indexBlockHandle blockHandle
	metaIndexBuilder := newMetaIndexBuilder()
	if err := b.writeFilterBlock(metaIndexBuilder); err != nil {
		return err
	}
	if err := b.writeIndexBlock(&indexBlockHandle); err != nil {
		return err
	}
	if err := b.writePropsBlock(metaIndexBuilder); err != nil {
		return err
	}
	if err := b.writeRawBlock(metaIndexBuilder.Finish(), CompressionNone, &metaIndexBlockHandle, false); err != nil {
		return err
	}

	// Write footer
	var footerBuf [footerEncodedLength]byte
	cursor := 0
	footerBuf[cursor] = byte(b.opts.ChecksumType)
	cursor += 1
	cursor += metaIndexBlockHandle.EncodeTo(footerBuf[cursor:])
	cursor += indexBlockHandle.EncodeTo(footerBuf[cursor:])
	cursor = footerEncodedLength - 12
	rocksEndian.PutUint32(footerBuf[cursor:], 2)
	cursor += 4
	rocksEndian.PutUint32(footerBuf[cursor:], blockBasedTableMagicNumber&0xffffffff)
	cursor += 4
	rocksEndian.PutUint32(footerBuf[cursor:], blockBasedTableMagicNumber>>32)

	if err := b.writer.Append(footerBuf[:]); err != nil {
		return err
	}
	b.offset += uint64(len(footerBuf))
	return b.writer.Flush(true)
}

func (b *BlockBasedTableBuilder) flush() error {
	if b.dataBlockBuilder.Empty() {
		return nil
	}
	if err := b.writeBlock(b.dataBlockBuilder.Finish(), &b.pendingHandle, true); err != nil {
		return err
	}

	b.props.DataSize = b.offset
	b.props.NumDataBlocks += 1
	b.dataBlockBuilder.Reset()

	return nil
}

func (b *BlockBasedTableBuilder) writeFilterBlock(metaIndexBuilder *metaIndexBuilder) error {
	if b.filterBuilder.Empty() {
		return nil
	}

	var filterBlockHandle blockHandle
	contents := b.filterBuilder.Finish()
	b.props.FilterSize += uint64(len(contents))
	if err := b.writeRawBlock(contents, CompressionNone, &filterBlockHandle, false); err != nil {
		return err
	}
	metaIndexBuilder.AddHandle(bloomBlockHandleKey, &filterBlockHandle)

	return nil
}

func (b *BlockBasedTableBuilder) writeIndexBlock(indexBlockHandle *blockHandle) error {
	contents := b.indexBlockBuilder.Finish()
	if b.opts.EnableIndexCompression {
		return b.writeBlock(contents, indexBlockHandle, false)
	}
	return b.writeRawBlock(contents, CompressionNone, indexBlockHandle, false)
}

func (b *BlockBasedTableBuilder) writePropsBlock(metaIndexBuilder *metaIndexBuilder) error {
	b.setupProperties()
	p := &b.props
	var handle blockHandle
	propsBuilder := newPropsBlockBuilder()
	for _, f := range b.opts.PropsInjectors {
		f(propsBuilder)
	}
	propsBuilder.AddUint64(propColumnFamilyId, p.ColumnFamilyID)
	propsBuilder.AddString(propCompression, p.CompressionName)
	propsBuilder.AddUint64(propCreationTime, p.CreationTime)
	propsBuilder.AddUint64(propDataSize, p.DataSize)
	if p.FilterSize != 0 {
		propsBuilder.AddString(propFilterPolicy, p.FilterPolicyName)
		propsBuilder.AddUint64(propFilterSize, p.FilterSize)
	}
	propsBuilder.AddUint64(propFixedKeyLength, 0)
	propsBuilder.AddUint64(propFormatVersion, 2)
	propsBuilder.AddUint64(propIndexKeyIsUserKey, 0)
	propsBuilder.AddUint64(propIndexSize, p.IndexSize)
	propsBuilder.AddUint64(propNumDataBlocks, p.NumDataBlocks)
	propsBuilder.AddUint64(propNumEntries, p.NumEntries)
	propsBuilder.AddUint64(propOldestKeyTime, p.OldestKeyTime)
	if p.PrefixExtractorName != "" {
		propsBuilder.AddString(propPrefixExtractorName, p.PrefixExtractorName)
	}
	propsBuilder.AddUint64(propRawKeySize, p.RawKeySize)
	propsBuilder.AddUint64(propRawValueSize, p.RawValueSize)

	contents := propsBuilder.Finish()
	if err := b.writeRawBlock(contents, CompressionNone, &handle, false); err != nil {
		return err
	}
	metaIndexBuilder.AddHandle(propsBlockHandleKey, &handle)

	return nil
}

func (b *BlockBasedTableBuilder) setupProperties() {
	p := &b.props
	p.ColumnFamilyID = math.MaxInt32
	p.ColumnFamilyName = ""
	p.FilterPolicyName = "rocksdb.BuiltinBloomFilter"
	p.IndexSize = uint64(b.indexBlockBuilder.IndexSize() + blockTrailerSize)
	p.CompressionName = b.opts.CompressionType.String()
	p.CreationTime = b.opts.CreationTime
	p.OldestKeyTime = b.opts.OldestKeyTime
	p.PrefixExtractorName = b.opts.PrefixExtractorName
}

func (b *BlockBasedTableBuilder) writeBlock(blockContents []byte, handle *blockHandle, isDataBlock bool) error {
	tp := b.opts.CompressionType
	compressedBlock, compressed := CompressBlock(tp, blockContents, b.compressBuf)
	if !compressed {
		return b.writeRawBlock(blockContents, CompressionNone, handle, isDataBlock)
	}
	b.compressBuf = compressedBlock

	return b.writeRawBlock(compressedBlock, tp, handle, isDataBlock)
}

func (b *BlockBasedTableBuilder) writeRawBlock(contents []byte, tp CompressionType, handle *blockHandle, isDataBlock bool) error {
	handle.Size = uint64(len(contents))
	handle.Offset = b.offset
	if err := b.writer.Append(contents); err != nil {
		return err
	}

	var trailer [blockTrailerSize]byte
	trailer[0] = byte(tp)
	switch b.opts.ChecksumType {
	case ChecksumNone:
		rocksEndian.PutUint32(trailer[1:], 0)
	case ChecksumCRC32:
		crc := newCrc32()
		crc.Write(contents)
		crc.Write(trailer[0:1])
		rocksEndian.PutUint32(trailer[1:], maskCrc32(crc.Sum32()))
	case ChecksumXXHash:
		panic("unsupported")
	}
	if err := b.writer.Append(trailer[:]); err != nil {
		return err
	}
	b.offset += uint64(len(contents) + blockTrailerSize)

	if b.opts.BlockAlign && isDataBlock {
		pad := (b.alignment - ((len(contents) + blockTrailerSize) & (b.alignment - 1))) & (b.alignment - 1)
		if err := b.writer.Append(make([]byte, pad)); err != nil {
			return err
		}
		b.offset += uint64(pad)
	}

	return nil
}

func (b *BlockBasedTableBuilder) shouldFlush(key, value []byte) bool {
	currSz := b.dataBlockBuilder.EstimateSize()
	if currSz == 0 {
		return false
	}
	if currSz >= b.opts.BlockSize {
		return true
	}

	newEstimatedSz := b.dataBlockBuilder.EstimateSizeAfterKV(key, value)

	if b.opts.BlockAlign {
		newEstimatedSz += blockTrailerSize
		return newEstimatedSz > b.opts.BlockSize
	}
	return newEstimatedSz > b.opts.BlockSize && currSz > b.blockSizeDeviationLimit
}

// Note: now assume format_version == 2
type indexBlockBuilder struct {
	blockBuilder blockBuilder
	indexSize    int
}

func newIndexBlockBuilder(restartInterval int) *indexBlockBuilder {
	b := new(indexBlockBuilder)
	b.blockBuilder.Init(restartInterval)
	return b
}

func (b *indexBlockBuilder) AddIndexEntry(lastKey []byte, handle *blockHandle) {
	b.blockBuilder.Add(lastKey, handle.Encode())
}

func (b *indexBlockBuilder) IndexSize() int {
	return b.indexSize
}

func (b *indexBlockBuilder) Finish() []byte {
	contents := b.blockBuilder.Finish()
	b.indexSize = len(contents)
	return contents
}

type metaIndexBuilder struct {
	blockBuilder blockBuilder
}

func newMetaIndexBuilder() *metaIndexBuilder {
	b := new(metaIndexBuilder)
	b.blockBuilder.Init(1)
	return b
}
func (b *metaIndexBuilder) AddHandle(key string, handle *blockHandle) {
	b.blockBuilder.Add([]byte(key), handle.Encode())
}

func (b *metaIndexBuilder) Finish() []byte {
	return b.blockBuilder.Finish()
}
