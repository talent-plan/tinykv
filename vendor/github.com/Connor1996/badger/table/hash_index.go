package table

import (
	"encoding/binary"
)

const (
	resultNoEntry  = 65535
	resultFallback = 65534
	maxBlockCnt    = 65533
)

type hashEntry struct {
	entryPosition
	hash uint64
}

type entryPosition struct {
	// blockIdx is the index of block which contains this key.
	blockIdx uint16

	// offset is the index of this key in block.
	offset uint8
}

func (e *entryPosition) encode() []byte {
	b := make([]byte, 3)
	binary.LittleEndian.PutUint16(b, e.blockIdx)
	b[2] = e.offset
	return b
}

func (e *entryPosition) decode(b []byte) {
	e.blockIdx = binary.LittleEndian.Uint16(b)
	e.offset = b[2]
}

func buildHashIndex(buf []byte, hashEntries []hashEntry, hashUtilRatio float32) []byte {
	if len(hashEntries) == 0 {
		return append(buf, u32ToBytes(0)...)
	}

	numBuckets := uint32(float32(len(hashEntries)) / hashUtilRatio)
	bufLen := len(buf)
	buf = append(buf, make([]byte, numBuckets*3+4)...)
	buckets := buf[bufLen:]
	for i := 0; i < int(numBuckets); i++ {
		binary.LittleEndian.PutUint16(buckets[i*3:], resultNoEntry)
	}

	for _, h := range hashEntries {
		idx := h.hash % uint64(numBuckets)
		bucket := buckets[idx*3 : (idx+1)*3]
		blkIdx := binary.LittleEndian.Uint16(bucket[:2])
		if blkIdx == resultNoEntry {
			binary.LittleEndian.PutUint16(bucket[:2], h.blockIdx)
			bucket[2] = h.offset
		} else if blkIdx != h.blockIdx {
			binary.LittleEndian.PutUint16(bucket[:2], resultFallback)
		}
	}
	copy(buckets[numBuckets*3:], u32ToBytes(numBuckets))

	return buf
}

type hashIndex struct {
	buckets    []byte
	numBuckets int
}

func (i *hashIndex) readIndex(buf []byte, numBucket int) {
	i.buckets = buf
	i.numBuckets = numBucket
}

func (i *hashIndex) lookup(keyHash uint64) (uint32, uint8) {
	if i.buckets == nil {
		return resultFallback, 0
	}
	idx := keyHash % uint64(i.numBuckets)
	buf := i.buckets[idx*3:]
	blkIdx := binary.LittleEndian.Uint16(buf)
	return uint32(blkIdx), uint8(buf[2])
}
