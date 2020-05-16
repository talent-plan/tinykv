package surf

import (
	"bytes"
	"io"
	"math/bits"
	"sort"

	"github.com/dgryski/go-farm"
)

type bitVector struct {
	numBits uint32
	bits    []uint64
}

func (v *bitVector) numWords() uint32 {
	wordSz := v.numBits / wordSize
	if v.numBits%wordSize != 0 {
		wordSz++
	}
	return wordSz
}

func (v *bitVector) bitsSize() uint32 {
	return v.numWords() * 8
}

func (v *bitVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	for _, n := range numBitsPerLevel {
		v.numBits += n
	}

	v.bits = make([]uint64, v.numWords())

	var wordID, bitShift uint32
	for level, bits := range bitsPerLevel {
		n := numBitsPerLevel[level]
		if n == 0 {
			continue
		}

		nCompleteWords := n / wordSize
		for word := 0; uint32(word) < nCompleteWords; word++ {
			v.bits[wordID] |= bits[word] << bitShift
			wordID++
			if bitShift > 0 {
				v.bits[wordID] |= bits[word] >> (wordSize - bitShift)
			}
		}

		remain := n % wordSize
		if remain > 0 {
			lastWord := bits[nCompleteWords]
			v.bits[wordID] |= lastWord << bitShift
			if bitShift+remain <= wordSize {
				bitShift = (bitShift + remain) % wordSize
				if bitShift == 0 {
					wordID++
				}
			} else {
				wordID++
				v.bits[wordID] |= lastWord >> (wordSize - bitShift)
				bitShift = bitShift + remain - wordSize
			}
		}
	}
}

func (v *bitVector) IsSet(pos uint32) bool {
	return readBit(v.bits, pos)
}

func (v *bitVector) DistanceToNextSetBit(pos uint32) uint32 {
	var distance uint32 = 1
	wordOff := (pos + 1) / wordSize
	bitsOff := (pos + 1) % wordSize

	if wordOff >= uint32(len(v.bits)) {
		return 0
	}

	testBits := v.bits[wordOff] >> bitsOff
	if testBits > 0 {
		return distance + uint32(bits.TrailingZeros64(testBits))
	}

	numWords := v.numWords()
	if wordOff == numWords-1 {
		return v.numBits - pos
	}
	distance += wordSize - bitsOff

	for wordOff < numWords-1 {
		wordOff++
		testBits = v.bits[wordOff]
		if testBits > 0 {
			return distance + uint32(bits.TrailingZeros64(testBits))
		}
		distance += wordSize
	}

	if wordOff == numWords-1 && v.numBits%64 != 0 {
		distance -= wordSize - v.numBits%64
	}

	return distance
}

func (v *bitVector) DistanceToPrevSetBit(pos uint32) uint32 {
	if pos == 0 {
		return 1
	}
	distance := uint32(1)
	wordOff := (pos - 1) / wordSize
	bitsOff := (pos - 1) % wordSize

	testBits := v.bits[wordOff] << (wordSize - 1 - bitsOff)
	if testBits > 0 {
		return distance + uint32(bits.LeadingZeros64(testBits))
	}
	distance += bitsOff + 1

	for wordOff > 0 {
		wordOff--
		testBits = v.bits[wordOff]
		if testBits > 0 {
			return distance + uint32(bits.LeadingZeros64(testBits))
		}
		distance += wordSize
	}
	return distance
}

type valueVector struct {
	bytes     []byte
	valueSize uint32
}

func (v *valueVector) Init(valuesPerLevel [][]byte, valueSize uint32) {
	var size int
	for l := range valuesPerLevel {
		size += len(valuesPerLevel[l])
	}
	v.valueSize = valueSize
	v.bytes = make([]byte, size)

	var pos uint32
	for _, val := range valuesPerLevel {
		copy(v.bytes[pos:], val)
		pos += uint32(len(val))
	}
}

func (v *valueVector) Get(pos uint32) []byte {
	off := pos * v.valueSize
	return v.bytes[off : off+v.valueSize]
}

func (v *valueVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *valueVector) rawMarshalSize() int64 {
	return 8 + int64(len(v.bytes))
}

func (v *valueVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.bytes)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	endian.PutUint32(bs[:], v.valueSize)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	if _, err := w.Write(v.bytes); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *valueVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	sz := int64(endian.Uint32(buf))
	cursor += 4

	v.valueSize = endian.Uint32(buf[cursor:])
	cursor += 4

	v.bytes = buf[cursor : cursor+sz]
	cursor = align(cursor + sz)

	return buf[cursor:]
}

const selectSampleInterval = 64

type selectVector struct {
	bitVector
	numOnes   uint32
	selectLut []uint32
}

func (v *selectVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *selectVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	lut := []uint32{0}
	sampledOnes := selectSampleInterval
	onesUptoWord := 0
	for i, w := range v.bits {
		ones := bits.OnesCount64(w)
		for sampledOnes <= onesUptoWord+ones {
			diff := sampledOnes - onesUptoWord
			targetPos := i*wordSize + int(select64(w, int64(diff)))
			lut = append(lut, uint32(targetPos))
			sampledOnes += selectSampleInterval
		}
		onesUptoWord += ones
	}

	v.numOnes = uint32(onesUptoWord)
	v.selectLut = make([]uint32, len(lut))
	for i := range v.selectLut {
		v.selectLut[i] = lut[i]
	}

	return v
}

func (v *selectVector) lutSize() uint32 {
	return (v.numOnes/selectSampleInterval + 1) * 4
}

// Select returns the postion of the rank-th 1 bit.
// position is zero-based; rank is one-based.
// E.g., for bitvector: 100101000, select(3) = 5
func (v *selectVector) Select(rank uint32) uint32 {
	lutIdx := rank / selectSampleInterval
	rankLeft := rank % selectSampleInterval
	if lutIdx == 0 {
		rankLeft--
	}

	pos := v.selectLut[lutIdx]
	if rankLeft == 0 {
		return pos
	}

	wordOff := pos / wordSize
	bitsOff := pos % wordSize
	if bitsOff == wordSize-1 {
		wordOff++
		bitsOff = 0
	} else {
		bitsOff++
	}

	w := v.bits[wordOff] >> bitsOff << bitsOff
	ones := uint32(bits.OnesCount64(w))
	for ones < rankLeft {
		wordOff++
		w = v.bits[wordOff]
		rankLeft -= ones
		ones = uint32(bits.OnesCount64(w))
	}

	return wordOff*wordSize + uint32(select64(w, int64(rankLeft)))
}

func (v *selectVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *selectVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *selectVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.numOnes)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.selectLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *selectVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.numOnes = endian.Uint32(buf[cursor:])
	cursor += 4

	bitsSize := int64(v.bitsSize())
	v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
	cursor += bitsSize

	lutSize := int64(v.lutSize())
	v.selectLut = bytesToU32Slice(buf[cursor : cursor+lutSize])
	cursor = align(cursor + lutSize)
	return buf[cursor:]
}

const (
	rankDenseBlockSize  = 64
	rankSparseBlockSize = 512
)

type rankVector struct {
	bitVector
	blockSize uint32
	rankLut   []uint32
}

func (v *rankVector) init(blockSize uint32, bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *rankVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	v.blockSize = blockSize
	wordPerBlk := v.blockSize / wordSize
	nblks := v.numBits/v.blockSize + 1
	v.rankLut = make([]uint32, nblks)

	var totalRank, i uint32
	for i = 0; i < nblks-1; i++ {
		v.rankLut[i] = totalRank
		totalRank += popcountBlock(v.bits, i*wordPerBlk, v.blockSize)
	}
	v.rankLut[nblks-1] = totalRank
	return v
}

func (v *rankVector) lutSize() uint32 {
	return (v.numBits/v.blockSize + 1) * 4
}

func (v *rankVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *rankVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *rankVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.blockSize)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.rankLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *rankVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.blockSize = endian.Uint32(buf[cursor:])
	cursor += 4

	bitsSize := int64(v.bitsSize())
	v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
	cursor += bitsSize

	lutSize := int64(v.lutSize())
	v.rankLut = bytesToU32Slice(buf[cursor : cursor+lutSize])
	cursor = align(cursor + lutSize)
	return buf[cursor:]
}

type rankVectorDense struct {
	rankVector
}

func (v *rankVectorDense) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	v.rankVector.init(rankDenseBlockSize, bitsPerLevel, numBitsPerLevel)
}

func (v *rankVectorDense) Rank(pos uint32) uint32 {
	wordPreBlk := uint32(rankDenseBlockSize / wordSize)
	blockOff := pos / rankDenseBlockSize
	bitsOff := pos % rankDenseBlockSize

	return v.rankLut[blockOff] + popcountBlock(v.bits, blockOff*wordPreBlk, bitsOff+1)
}

type rankVectorSparse struct {
	rankVector
}

func (v *rankVectorSparse) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	v.rankVector.init(rankSparseBlockSize, bitsPerLevel, numBitsPerLevel)
}

func (v *rankVectorSparse) Rank(pos uint32) uint32 {
	wordPreBlk := uint32(rankSparseBlockSize / wordSize)
	blockOff := pos / rankSparseBlockSize
	bitsOff := pos % rankSparseBlockSize

	return v.rankLut[blockOff] + popcountBlock(v.bits, blockOff*wordPreBlk, bitsOff+1)
}

const labelTerminator = 0xff

type labelVector struct {
	labels []byte
}

func (v *labelVector) Init(labelsPerLevel [][]byte, startLevel, endLevel uint32) {
	numBytes := 1
	for l := startLevel; l < endLevel; l++ {
		numBytes += len(labelsPerLevel[l])
	}
	v.labels = make([]byte, numBytes)

	var pos uint32
	for l := startLevel; l < endLevel; l++ {
		copy(v.labels[pos:], labelsPerLevel[l])
		pos += uint32(len(labelsPerLevel[l]))
	}
}

func (v *labelVector) GetLabel(pos uint32) byte {
	return v.labels[pos]
}

func (v *labelVector) Search(k byte, off, size uint32) (uint32, bool) {
	start := off
	if size > 1 && v.labels[start] == labelTerminator {
		start++
		size--
	}

	end := start + size
	if end > uint32(len(v.labels)) {
		end = uint32(len(v.labels))
	}
	result := bytes.IndexByte(v.labels[start:end], k)
	if result < 0 {
		return off, false
	}
	return start + uint32(result), true
}

func (v *labelVector) SearchGreaterThan(label byte, pos, size uint32) (uint32, bool) {
	if size > 1 && v.labels[pos] == labelTerminator {
		pos++
		size--
	}

	result := sort.Search(int(size), func(i int) bool { return v.labels[pos+uint32(i)] > label })
	if uint32(result) == size {
		return pos + uint32(result) - 1, false
	}
	return pos + uint32(result), true
}

func (v *labelVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *labelVector) rawMarshalSize() int64 {
	return 4 + int64(len(v.labels))
}

func (v *labelVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.labels)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.labels); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *labelVector) Unmarshal(buf []byte) []byte {
	l := endian.Uint32(buf)
	v.labels = buf[4 : 4+l]
	return buf[align(int64(4+l)):]
}

const (
	hashShift       = 7
	couldBePositive = 2
)

// max(hashSuffixLen + realSuffixLen) = 64 bits
// For real suffixes, if the stored key is not long enough to provide
// realSuffixLen suffix bits, its suffix field is cleared (i.e., all 0's)
// to indicate that there is no suffix info associated with the key.
type suffixVector struct {
	bitVector
	hashSuffixLen uint32
	realSuffixLen uint32
}

func (v *suffixVector) Init(hashLen, realLen uint32, bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *suffixVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	v.hashSuffixLen = hashLen
	v.realSuffixLen = realLen
	return v
}

func (v *suffixVector) CheckEquality(idx uint32, key []byte, level uint32) bool {
	if !v.hasSuffix() {
		return true
	}
	if idx*v.suffixLen() >= v.numBits {
		return false
	}

	suffix := v.read(idx)
	if v.isRealSuffix() {
		if suffix == 0 {
			return true
		}
		if uint32(len(key)) < level || (uint32(len(key))-level)*8 < v.realSuffixLen {
			return false
		}
	}
	expected := constructSuffix(key, level, v.realSuffixLen, v.hashSuffixLen)
	return suffix == expected
}

func (v *suffixVector) Compare(key []byte, idx, level uint32) int {
	if idx*v.suffixLen() >= v.numBits || v.realSuffixLen == 0 {
		return couldBePositive
	}

	suffix := v.read(idx)
	if v.isMixedSuffix() {
		suffix = extractRealSuffix(suffix, v.realSuffixLen)
	}
	expected := constructRealSuffix(key, level, v.realSuffixLen)

	if suffix == 0 || expected == 0 {
		// Key length is not long enough to provide suffix, cannot determin which one is the larger one.
		return couldBePositive
	} else if suffix < expected {
		return -1
	} else if suffix == expected {
		return couldBePositive
	} else {
		return 1
	}
}

func (v *suffixVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *suffixVector) rawMarshalSize() int64 {
	return 4 + 4 + 4 + int64(v.bitsSize())
}

func (v *suffixVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.hashSuffixLen)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.realSuffixLen)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *suffixVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.hashSuffixLen = endian.Uint32(buf[cursor:])
	cursor += 4
	v.realSuffixLen = endian.Uint32(buf[cursor:])
	cursor += 4
	if v.hasSuffix() {
		bitsSize := int64(v.bitsSize())
		v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
		cursor += bitsSize
	}
	cursor = align(cursor)
	return buf[cursor:]
}

func (v *suffixVector) read(idx uint32) uint64 {
	suffixLen := v.suffixLen()
	bitPos := idx * suffixLen
	wordOff := bitPos / wordSize
	bitsOff := bitPos % wordSize
	result := (v.bits[wordOff] >> bitsOff) & (1<<suffixLen - 1)
	if bitsOff+suffixLen > wordSize {
		leftLen := wordSize - bitsOff
		rightLen := suffixLen - leftLen
		result |= (v.bits[wordOff+1] & (1<<rightLen - 1)) << leftLen
	}
	return result
}

func (v *suffixVector) suffixLen() uint32 {
	return v.hashSuffixLen + v.realSuffixLen
}

func (v *suffixVector) hasSuffix() bool {
	return v.realSuffixLen != 0 || v.hashSuffixLen != 0
}

func (v *suffixVector) isHashSuffix() bool {
	return v.realSuffixLen == 0 && v.hashSuffixLen != 0
}

func (v *suffixVector) isRealSuffix() bool {
	return v.realSuffixLen != 0 && v.hashSuffixLen == 0
}

func (v *suffixVector) isMixedSuffix() bool {
	return v.realSuffixLen != 0 && v.hashSuffixLen != 0
}

func constructSuffix(key []byte, level uint32, realSuffixLen, hashSuffixLen uint32) uint64 {
	if hashSuffixLen == 0 && realSuffixLen == 0 {
		return 0
	}
	if realSuffixLen == 0 {
		return constructHashSuffix(key, hashSuffixLen)
	}
	if hashSuffixLen == 0 {
		return constructRealSuffix(key, level, realSuffixLen)
	}
	return constructMixedSuffix(key, level, realSuffixLen, hashSuffixLen)
}

func constructHashSuffix(key []byte, hashSuffixLen uint32) uint64 {
	fp := farm.Fingerprint64(key)
	fp <<= wordSize - hashSuffixLen - hashShift
	fp >>= wordSize - hashSuffixLen
	return fp
}

func constructRealSuffix(key []byte, level, realSuffixLen uint32) uint64 {
	klen := uint32(len(key))
	if klen < level || (klen-level)*8 < realSuffixLen {
		return 0
	}

	var suffix uint64
	nbytes := realSuffixLen / 8
	if nbytes > 0 {
		suffix += uint64(key[level])
		for i := 1; uint32(i) < nbytes; i++ {
			suffix <<= 8
			suffix += uint64(key[i])
		}
	}

	off := realSuffixLen % 8
	if off > 0 {
		suffix <<= off
		remain := uint64(key[level+nbytes])
		remain >>= 8 - off
		suffix += remain
	}

	return suffix
}

func constructMixedSuffix(key []byte, level, realSuffixLen, hashSuffixLen uint32) uint64 {
	hs := constructHashSuffix(key, hashSuffixLen)
	rs := constructRealSuffix(key, level, realSuffixLen)
	return (hs << realSuffixLen) | rs
}

func extractRealSuffix(suffix uint64, suffixLen uint32) uint64 {
	mask := (uint64(1) << suffixLen) - 1
	return suffix & mask
}

type prefixVector struct {
	hasPrefixVec  rankVectorSparse
	prefixOffsets []uint32
	prefixData    []byte
}

func (v *prefixVector) Init(hasPrefixBits [][]uint64, numNodesPerLevel []uint32, prefixes [][][]byte) {
	v.hasPrefixVec.Init(hasPrefixBits, numNodesPerLevel)

	var offset uint32
	for _, level := range prefixes {
		for _, prefix := range level {
			v.prefixOffsets = append(v.prefixOffsets, offset)
			offset += uint32(len(prefix))
			v.prefixData = append(v.prefixData, prefix...)
		}
	}
}

func (v *prefixVector) CheckPrefix(key []byte, depth uint32, nodeID uint32) (uint32, bool) {
	prefix := v.GetPrefix(nodeID)
	if len(prefix) == 0 {
		return 0, true
	}

	if int(depth)+len(prefix) > len(key) {
		return 0, false
	}
	if !bytes.Equal(key[depth:depth+uint32(len(prefix))], prefix) {
		return 0, false
	}
	return uint32(len(prefix)), true
}

func (v *prefixVector) GetPrefix(nodeID uint32) []byte {
	if !v.hasPrefixVec.IsSet(nodeID) {
		return nil
	}

	prefixID := v.hasPrefixVec.Rank(nodeID) - 1
	start := v.prefixOffsets[prefixID]
	end := uint32(len(v.prefixData))
	if int(prefixID+1) < len(v.prefixOffsets) {
		end = v.prefixOffsets[prefixID+1]
	}
	return v.prefixData[start:end]
}

func (v *prefixVector) WriteTo(w io.Writer) error {
	if err := v.hasPrefixVec.WriteTo(w); err != nil {
		return err
	}

	var length [8]byte
	endian.PutUint32(length[:4], uint32(len(v.prefixOffsets)*4))
	endian.PutUint32(length[4:], uint32(len(v.prefixData)))

	if _, err := w.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.prefixOffsets)); err != nil {
		return err
	}
	if _, err := w.Write(v.prefixData); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *prefixVector) Unmarshal(b []byte) []byte {
	buf1 := v.hasPrefixVec.Unmarshal(b)
	var cursor int64
	offsetsLen := int64(endian.Uint32(buf1[cursor:]))
	cursor += 4
	dataLen := int64(endian.Uint32(buf1[cursor:]))
	cursor += 4

	v.prefixOffsets = bytesToU32Slice(buf1[cursor : cursor+offsetsLen])
	cursor += offsetsLen
	v.prefixData = buf1[cursor : cursor+dataLen]

	return b[v.MarshalSize():]
}

func (v *prefixVector) rawMarshalSize() int64 {
	return v.hasPrefixVec.MarshalSize() + 8 + int64(len(v.prefixOffsets)*4+len(v.prefixData))
}

func (v *prefixVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}
