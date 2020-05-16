package surf

import (
	"bytes"
	"io"
)

const (
	denseFanout      = 256
	denseRankBlkSize = 512
)

type loudsDense struct {
	labelVec    rankVectorDense
	hasChildVec rankVectorDense
	isPrefixVec rankVectorDense
	suffixes    suffixVector
	values      valueVector
	prefixVec   prefixVector

	// height is dense end level.
	height uint32
}

func (ld *loudsDense) Init(builder *Builder) *loudsDense {
	ld.height = builder.sparseStartLevel

	numBitsPerLevel := make([]uint32, ld.height)
	for level := range numBitsPerLevel {
		numBitsPerLevel[level] = uint32(len(builder.ldLabels[level]) * wordSize)
	}

	ld.labelVec.Init(builder.ldLabels[:ld.height], numBitsPerLevel)
	ld.hasChildVec.Init(builder.ldHasChild[:ld.height], numBitsPerLevel)
	ld.isPrefixVec.Init(builder.ldIsPrefix[:ld.height], builder.nodeCounts)

	if builder.suffixLen() != 0 {
		hashLen := builder.hashSuffixLen
		realLen := builder.realSuffixLen
		suffixLen := hashLen + realLen
		numSuffixBitsPerLevel := make([]uint32, ld.height)
		for i := range numSuffixBitsPerLevel {
			numSuffixBitsPerLevel[i] = builder.suffixCounts[i] * suffixLen
		}
		ld.suffixes.Init(hashLen, realLen, builder.suffixes[:ld.height], numSuffixBitsPerLevel)
	}

	ld.values.Init(builder.values[:ld.height], builder.valueSize)
	ld.prefixVec.Init(builder.hasPrefix[:ld.height], builder.nodeCounts[:ld.height], builder.prefixes[:ld.height])

	return ld
}

func (ld *loudsDense) Get(key []byte) (sparseNode int64, depth uint32, value []byte, ok bool) {
	var nodeID, pos uint32
	for level := uint32(0); level < ld.height; level++ {
		prefixLen, ok := ld.prefixVec.CheckPrefix(key, depth, nodeID)
		if !ok {
			return -1, depth, nil, false
		}
		depth += prefixLen

		pos = nodeID * denseFanout
		if depth >= uint32(len(key)) {
			if ok = ld.isPrefixVec.IsSet(nodeID); ok {
				valPos := ld.suffixPos(pos, true)
				if ok = ld.suffixes.CheckEquality(valPos, key, depth+1); ok {
					value = ld.values.Get(valPos)
				}
			}
			return -1, depth, value, ok
		}
		pos += uint32(key[depth])

		if !ld.labelVec.IsSet(pos) {
			return -1, depth, nil, false
		}

		if !ld.hasChildVec.IsSet(pos) {
			valPos := ld.suffixPos(pos, false)
			if ok = ld.suffixes.CheckEquality(valPos, key, depth+1); ok {
				value = ld.values.Get(valPos)
			}
			return -1, depth, value, ok
		}

		nodeID = ld.childNodeID(pos)
		depth++
	}

	return int64(nodeID), depth, nil, true
}

func (ld *loudsDense) MarshalSize() int64 {
	return align(ld.rawMarshalSize())
}

func (ld *loudsDense) rawMarshalSize() int64 {
	return 4 + ld.labelVec.MarshalSize() + ld.hasChildVec.MarshalSize() + ld.isPrefixVec.MarshalSize() + ld.suffixes.MarshalSize() + ld.prefixVec.MarshalSize()
}

func (ld *loudsDense) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], ld.height)

	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if err := ld.labelVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.hasChildVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.isPrefixVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.suffixes.WriteTo(w); err != nil {
		return err
	}
	if err := ld.prefixVec.WriteTo(w); err != nil {
		return err
	}

	padding := ld.MarshalSize() - ld.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (ld *loudsDense) Unmarshal(buf []byte) []byte {
	ld.height = endian.Uint32(buf)
	buf1 := buf[4:]
	buf1 = ld.labelVec.Unmarshal(buf1)
	buf1 = ld.hasChildVec.Unmarshal(buf1)
	buf1 = ld.isPrefixVec.Unmarshal(buf1)
	buf1 = ld.suffixes.Unmarshal(buf1)
	buf1 = ld.prefixVec.Unmarshal(buf1)

	sz := align(int64(len(buf) - len(buf1)))
	return buf[sz:]
}

func (ld *loudsDense) childNodeID(pos uint32) uint32 {
	return ld.hasChildVec.Rank(pos)
}

func (ld *loudsDense) suffixPos(pos uint32, isPrefix bool) uint32 {
	nodeID := pos / denseFanout
	suffixPos := ld.labelVec.Rank(pos) - ld.hasChildVec.Rank(pos) + ld.isPrefixVec.Rank(nodeID) - 1

	// Correct off by one error when current have a leaf node at label 0.
	// Otherwise suffixPos will point to that leaf node's suffix.
	if isPrefix && ld.labelVec.IsSet(pos) && !ld.hasChildVec.IsSet(pos) {
		suffixPos--
	}
	return suffixPos
}

func (ld *loudsDense) nextPos(pos uint32) uint32 {
	return pos + ld.labelVec.DistanceToNextSetBit(pos)
}

func (ld *loudsDense) prevPos(pos uint32) (uint32, bool) {
	dist := ld.labelVec.DistanceToPrevSetBit(pos)
	if pos < dist {
		return 0, true
	}
	return pos - dist, false
}

type denseIter struct {
	valid         bool
	searchComp    bool
	leftComp      bool
	rightComp     bool
	ld            *loudsDense
	sendOutNodeID uint32
	sendOutDepth  uint32
	keyBuf        []byte
	level         uint32
	posInTrie     []uint32
	prefixLen     []uint32
	atPrefixKey   bool
}

func (it *denseIter) Init(ld *loudsDense) {
	it.ld = ld
	it.posInTrie = make([]uint32, ld.height)
	it.prefixLen = make([]uint32, ld.height)
}

func (it *denseIter) Reset() {
	it.valid = false
	it.level = 0
	it.atPrefixKey = false
	it.keyBuf = it.keyBuf[:0]
}

func (it *denseIter) Next() {
	if it.ld.height == 0 {
		return
	}
	if it.atPrefixKey {
		it.atPrefixKey = false
		it.MoveToLeftMostKey()
		return
	}

	pos := it.posInTrie[it.level]
	nextPos := it.ld.nextPos(pos)

	for pos == nextPos || nextPos/denseFanout > pos/denseFanout {
		if it.level == 0 {
			it.valid = false
			return
		}
		it.level--
		pos = it.posInTrie[it.level]
		nextPos = it.ld.nextPos(pos)
	}
	it.setAt(it.level, nextPos)
	it.MoveToLeftMostKey()
}

func (it *denseIter) Prev() {
	if it.ld.height == 0 {
		return
	}
	if it.atPrefixKey {
		it.atPrefixKey = false
		it.level--
	}
	pos := it.posInTrie[it.level]
	prevPos, out := it.ld.prevPos(pos)
	if out {
		it.valid = false
		return
	}

	for prevPos/denseFanout < pos/denseFanout {
		nodeID := pos / denseFanout
		if it.ld.isPrefixVec.IsSet(nodeID) {
			it.truncate(it.level)
			it.atPrefixKey = true
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		if it.level == 0 {
			it.valid = false
			return
		}
		it.level--
		pos = it.posInTrie[it.level]
		prevPos, out = it.ld.prevPos(pos)
		if out {
			it.valid = false
			return
		}
	}
	it.setAt(it.level, prevPos)
	it.MoveToRightMostKey()
}

func (it *denseIter) Seek(key []byte) bool {
	var nodeID, pos, depth uint32
	for it.level = 0; it.level < it.ld.height; it.level++ {
		prefix := it.ld.prefixVec.GetPrefix(nodeID)
		var prefixCmp int
		if len(prefix) != 0 {
			end := int(depth) + len(prefix)
			if end > len(key) {
				end = len(key)
			}
			prefixCmp = bytes.Compare(prefix, key[depth:end])
		}

		if prefixCmp < 0 {
			if nodeID == 0 {
				it.valid = false
				return false
			}
			it.level--
			it.Next()
			return false
		}

		pos = nodeID * denseFanout
		depth += uint32(len(prefix))
		if depth >= uint32(len(key)) || prefixCmp > 0 {
			if pos > 0 {
				it.append(it.ld.nextPos(pos - 1))
			} else {
				it.SetToFirstInRoot()
			}
			if it.ld.isPrefixVec.IsSet(nodeID) {
				it.atPrefixKey = true
				it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			} else {
				it.MoveToLeftMostKey()
			}
			return prefixCmp == 0
		}

		pos += uint32(key[depth])
		it.append(pos)
		depth++

		if !it.ld.labelVec.IsSet(pos) {
			it.Next()
			return false
		}

		if !it.ld.hasChildVec.IsSet(pos) {
			return it.compareSuffixGreaterThan(key, pos, depth)
		}

		nodeID = it.ld.childNodeID(pos)
	}

	it.level--
	it.sendOutNodeID = nodeID
	it.sendOutDepth = depth
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, false, true, true
	return true
}

func (it *denseIter) Key() []byte {
	if it.atPrefixKey {
		return it.keyBuf[:len(it.keyBuf)-1]
	}
	return it.keyBuf
}

func (it *denseIter) Value() []byte {
	valPos := it.ld.suffixPos(it.posInTrie[it.level], it.atPrefixKey)
	return it.ld.values.Get(valPos)
}

func (it *denseIter) Compare(key []byte) int {
	itKey := it.Key()

	cmpLen := len(itKey)
	if cmpLen > len(key) {
		cmpLen = len(key)
	}
	cmp := bytes.Compare(itKey[:cmpLen], key[:cmpLen])
	if cmp != 0 {
		return cmp
	}
	if len(itKey) > len(key) {
		return 1
	}
	if len(itKey) == len(key) && it.atPrefixKey {
		return 0
	}

	if it.IsComplete() {
		suffixPos := it.ld.suffixPos(it.posInTrie[it.level], it.atPrefixKey)
		return it.ld.suffixes.Compare(key, suffixPos, uint32(len(itKey)))
	}
	return cmp
}

func (it *denseIter) IsComplete() bool {
	return it.searchComp && (it.leftComp && it.rightComp)
}

func (it *denseIter) append(pos uint32) {
	nodeID := pos / denseFanout
	prefix := it.ld.prefixVec.GetPrefix(nodeID)
	it.keyBuf = append(it.keyBuf, prefix...)
	it.keyBuf = append(it.keyBuf, byte(pos%denseFanout))
	it.posInTrie[it.level] = pos
	it.prefixLen[it.level] = uint32(len(prefix)) + 1
	if it.level != 0 {
		it.prefixLen[it.level] += it.prefixLen[it.level-1]
	}
}

func (it *denseIter) MoveToLeftMostKey() {
	pos := it.posInTrie[it.level]
	if !it.ld.hasChildVec.IsSet(pos) {
		it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
		return
	}

	for it.level < it.ld.height-1 {
		it.level++
		nodeID := it.ld.childNodeID(pos)
		if it.ld.isPrefixVec.IsSet(nodeID) {
			it.append(it.ld.nextPos(nodeID*denseFanout - 1))
			it.atPrefixKey = true
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		pos = it.ld.nextPos(nodeID*denseFanout - 1)
		it.append(pos)

		// If trie branch terminates
		if !it.ld.hasChildVec.IsSet(pos) {
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}
	}
	it.sendOutNodeID = it.ld.childNodeID(pos)
	it.sendOutDepth = uint32(len(it.keyBuf))
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, false, true
}

func (it *denseIter) MoveToRightMostKey() {
	pos := it.posInTrie[it.level]
	if !it.ld.hasChildVec.IsSet(pos) {
		it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
		return
	}

	var out bool
	for it.level < it.ld.height-1 {
		it.level++
		nodeID := it.ld.childNodeID(pos)
		pos, out = it.ld.prevPos((nodeID + 1) * denseFanout)
		if out {
			it.valid = false
			return
		}
		it.append(pos)

		// If trie branch terminates
		if !it.ld.hasChildVec.IsSet(pos) {
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}
	}
	it.sendOutNodeID = it.ld.childNodeID(pos)
	it.sendOutDepth = uint32(len(it.keyBuf))
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, false
}

func (it *denseIter) SetToFirstInRoot() {
	if it.ld.labelVec.IsSet(0) {
		it.append(0)
	} else {
		it.append(it.ld.nextPos(0))
	}
}

func (it *denseIter) SetToLastInRoot() {
	pos, _ := it.ld.prevPos(denseFanout)
	it.append(pos)
}

func (it *denseIter) setAt(level, pos uint32) {
	it.keyBuf = append(it.keyBuf[:it.prefixLen[level]-1], byte(pos%denseFanout))
	it.posInTrie[it.level] = pos
}

func (it *denseIter) truncate(level uint32) {
	it.keyBuf = it.keyBuf[:it.prefixLen[level]]
}

func (it *denseIter) compareSuffixGreaterThan(key []byte, pos, level uint32) bool {
	cmp := it.ld.suffixes.Compare(key, it.ld.suffixPos(pos, false), level)
	if cmp < 0 {
		it.Next()
		return false
	}
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
	return cmp == couldBePositive
}
