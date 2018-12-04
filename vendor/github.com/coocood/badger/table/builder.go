/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table

import (
	"encoding/binary"
	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/options"
	"golang.org/x/time/rate"
	"os"
	"reflect"
	"unsafe"

	"github.com/coocood/badger/y"
	"github.com/coocood/bbloom"
)

const restartInterval = 256 // Might want to change this to be based on total size instead of numKeys.

type header struct {
	baseLen uint16 // Overlap with base key.
	diffLen uint16 // Length of the diff.
}

// Encode encodes the header.
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) {
	*h = *(*header)(unsafe.Pointer(&buf[0]))
}

const headerSize = 4

// Builder is used in building a table.
type Builder struct {
	counter int // Number of keys written for the current block.

	w          *fileutil.BufferedFileWriter
	buf        []byte
	writtenLen int

	baseKeysBuf     []byte
	baseKeysEndOffs []uint32

	blockBaseKey    []byte // Base key for the current block.
	blockBaseOffset uint32 // Offset for the current block.

	blockEndOffsets []uint32 // Base offsets of every block.

	// end offsets of every entry within the current block being built.
	// The offsets are relative to the start of the block.
	entryEndOffsets []uint32

	bloomFilter bbloom.Bloom

	enableHashIndex  bool
	hashIndexBuilder hashIndexBuilder
}

// NewTableBuilder makes a new TableBuilder.
// If the limiter is nil, the write speed during table build will not be limited.
func NewTableBuilder(f *os.File, limiter *rate.Limiter, opt options.TableBuilderOptions) *Builder {
	assumeKeyNum := 256 * 1024
	return &Builder{
		w:           fileutil.NewBufferedFileWriter(f, opt.WriteBufferSize, opt.BytesPerSync, limiter),
		buf:         make([]byte, 0, 4*1024),
		baseKeysBuf: make([]byte, 0, assumeKeyNum/restartInterval),
		// assume a large enough num of keys to init bloom filter.
		bloomFilter:      bbloom.New(float64(assumeKeyNum), 0.01),
		enableHashIndex:  opt.EnableHashIndex,
		hashIndexBuilder: newHashIndexBuilder(opt.HashUtilRatio),
	}
}

// Reset this builder with new file.
func (b *Builder) Reset(f *os.File) {
	b.resetBuffers()
	b.w.Reset(f)
}

// Reset this builder with new file and rate limiter.
func (b *Builder) ResetWithLimiter(f *os.File, limiter *rate.Limiter) {
	b.resetBuffers()
	b.w.ResetWithLimiter(f, limiter)
}

func (b *Builder) resetBuffers() {
	b.counter = 0
	b.buf = b.buf[:0]
	b.writtenLen = 0
	b.baseKeysBuf = b.baseKeysBuf[:0]
	b.baseKeysEndOffs = b.baseKeysEndOffs[:0]
	b.blockBaseKey = b.blockBaseKey[:0]
	b.blockBaseOffset = 0
	b.blockEndOffsets = b.blockEndOffsets[:0]
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.bloomFilter.Clear()
	b.hashIndexBuilder.reset()
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.writtenLen+len(b.buf) == 0 }

// keyDiff returns a suffix of newKey that is different from b.blockBaseKey.
func (b Builder) keyDiff(newKey []byte) []byte {
	for i := 0; i < len(newKey) && i < len(b.blockBaseKey); i++ {
		if newKey[i] != b.blockBaseKey[i] {
			return newKey[i:]
		}
	}
	return newKey
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	// Add key to bloom filter.
	if len(key) > 0 {
		keyNoTs := y.ParseKey(key)
		b.bloomFilter.Add(keyNoTs)
		if b.enableHashIndex {
			b.hashIndexBuilder.addKey(keyNoTs, uint32(len(b.baseKeysEndOffs)), uint8(b.counter))
		}
	}

	// diffKey stores the difference of key with blockBaseKey.
	var diffKey []byte
	if len(b.blockBaseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.blockBaseKey = append(b.blockBaseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		baseLen: uint16(len(key) - len(diffKey)),
		diffLen: uint16(len(diffKey)),
	}
	b.buf = append(b.buf, h.Encode()...)
	b.buf = append(b.buf, diffKey...) // We only need to store the key difference.
	b.buf = v.EncodeTo(b.buf)
	b.entryEndOffsets = append(b.entryEndOffsets, uint32(b.writtenLen+len(b.buf))-b.blockBaseOffset)
	b.counter++ // Increment number of keys added for this current block.
}

func (b *Builder) finishBlock() error {
	b.buf = append(b.buf, u32SliceToBytes(b.entryEndOffsets)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.entryEndOffsets)))...)
	b.blockEndOffsets = append(b.blockEndOffsets, uint32(b.writtenLen+len(b.buf)))

	// Add base key.
	b.baseKeysBuf = append(b.baseKeysBuf, b.blockBaseKey...)
	b.baseKeysEndOffs = append(b.baseKeysEndOffs, uint32(len(b.baseKeysBuf)))

	// Reset the block for the next build.
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.counter = 0
	b.blockBaseKey = b.blockBaseKey[:0]
	b.blockBaseOffset = uint32(b.writtenLen + len(b.buf))
	b.writtenLen += len(b.buf)
	if err := b.w.Append(b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	return nil
}

// Add adds a key-value pair to the block.
// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
func (b *Builder) Add(key []byte, value y.ValueStruct) error {
	if b.counter >= restartInterval {
		if err := b.finishBlock(); err != nil {
			return err
		}
	}
	b.addHelper(key, value)
	return nil // Currently, there is no meaningful error.
}

// TODO: vvv this was the comment on ReachedCapacity.
// FinalSize returns the *rough* final size of the array, counting the header which is not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(capacity int64) bool {
	estimateSz := b.writtenLen + len(b.buf) +
		4*len(b.blockEndOffsets) +
		len(b.baseKeysBuf) +
		4*len(b.baseKeysEndOffs)
	return int64(estimateSz) > capacity
}

// Finish finishes the table by appending the index.
func (b *Builder) Finish() error {
	b.finishBlock() // This will never start a new block.
	b.buf = append(b.buf, u32SliceToBytes(b.blockEndOffsets)...)
	b.buf = append(b.buf, b.baseKeysBuf...)
	b.buf = append(b.buf, u32SliceToBytes(b.baseKeysEndOffs)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.baseKeysEndOffs)))...)

	// Write bloom filter.
	bfData := b.bloomFilter.BinaryMarshal()
	b.buf = append(b.buf, bfData...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(bfData)))...)

	if b.enableHashIndex {
		b.buf = b.hashIndexBuilder.finish(b.buf)
	} else {
		b.buf = append(b.buf, u32ToBytes(0)...)
	}

	return b.w.FlushWithData(b.buf, true)
}

func u32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.LittleEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytesToU32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
