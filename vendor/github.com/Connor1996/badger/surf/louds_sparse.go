package surf

import (
	"bytes"
	"io"
)

type loudsSparse struct {
	height          uint32
	startLevel      uint32
	denseNodeCount  uint32
	denseChildCount uint32

	labelVec    labelVector
	hasChildVec rankVectorSparse
	loudsVec    selectVector
	suffixes    suffixVector
	values      valueVector
	prefixVec   prefixVector
}

func (ls *loudsSparse) Init(builder *Builder) *loudsSparse {
	ls.height = uint32(len(builder.lsLabels))
	ls.startLevel = builder.sparseStartLevel

	for l := 0; uint32(l) < ls.startLevel; l++ {
		ls.denseNodeCount += builder.nodeCounts[l]
	}

	if ls.startLevel != 0 {
		ls.denseChildCount = ls.denseNodeCount + builder.nodeCounts[ls.startLevel] - 1
	}

	ls.labelVec.Init(builder.lsLabels, ls.startLevel, ls.height)

	numItemsPerLevel := make([]uint32, ls.sparseLevels())
	for level := range numItemsPerLevel {
		numItemsPerLevel[level] = uint32(len(builder.lsLabels[int(ls.startLevel)+level]))
	}
	ls.hasChildVec.Init(builder.lsHasChild[ls.startLevel:], numItemsPerLevel)
	ls.loudsVec.Init(builder.lsLoudsBits[ls.startLevel:], numItemsPerLevel)

	if builder.suffixLen() != 0 {
		hashLen := builder.hashSuffixLen
		realLen := builder.realSuffixLen
		suffixLen := hashLen + realLen
		numSuffixBitsPerLevel := make([]uint32, ls.sparseLevels())
		for i := range numSuffixBitsPerLevel {
			numSuffixBitsPerLevel[i] = builder.suffixCounts[int(ls.startLevel)+i] * suffixLen
		}
		ls.suffixes.Init(hashLen, realLen, builder.suffixes[ls.startLevel:], numSuffixBitsPerLevel)
	}

	ls.values.Init(builder.values[ls.startLevel:], builder.valueSize)
	ls.prefixVec.Init(builder.hasPrefix[ls.startLevel:], builder.nodeCounts[ls.startLevel:], builder.prefixes[ls.startLevel:])

	return ls
}

func (ls *loudsSparse) Get(key []byte, startDepth, nodeID uint32) (value []byte, ok bool) {
	var (
		pos       = ls.firstLabelPos(nodeID)
		depth     uint32
		prefixLen uint32
	)
	for depth = startDepth; depth < uint32(len(key)); depth++ {
		prefixLen, ok = ls.prefixVec.CheckPrefix(key, depth, ls.prefixID(nodeID))
		if !ok {
			return nil, false
		}
		depth += prefixLen

		if depth >= uint32(len(key)) {
			break
		}

		if pos, ok = ls.labelVec.Search(key[depth], pos, ls.nodeSize(pos)); !ok {
			return nil, false
		}

		if !ls.hasChildVec.IsSet(pos) {
			valPos := ls.suffixPos(pos)
			if ok = ls.suffixes.CheckEquality(valPos, key, depth+1); ok {
				value = ls.values.Get(valPos)
			}
			return value, ok
		}

		nodeID = ls.childNodeID(pos)
		pos = ls.firstLabelPos(nodeID)
	}

	if ls.labelVec.GetLabel(pos) == labelTerminator && !ls.hasChildVec.IsSet(pos) {
		valPos := ls.suffixPos(pos)
		if ok = ls.suffixes.CheckEquality(valPos, key, depth+1); ok {
			value = ls.values.Get(valPos)
		}
		return value, ok
	}

	return nil, false
}

func (ls *loudsSparse) MarshalSize() int64 {
	return align(ls.rawMarshalSize())
}

func (ls *loudsSparse) rawMarshalSize() int64 {
	return 4*4 + ls.labelVec.MarshalSize() + ls.hasChildVec.MarshalSize() + ls.loudsVec.MarshalSize() +
		ls.suffixes.MarshalSize() + ls.prefixVec.MarshalSize()
}

func (ls *loudsSparse) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], ls.height)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.startLevel)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.denseNodeCount)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	endian.PutUint32(bs[:], ls.denseChildCount)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if err := ls.labelVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.hasChildVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.loudsVec.WriteTo(w); err != nil {
		return err
	}
	if err := ls.suffixes.WriteTo(w); err != nil {
		return err
	}
	if err := ls.prefixVec.WriteTo(w); err != nil {
		return err
	}

	padding := ls.MarshalSize() - ls.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (ls *loudsSparse) Unmarshal(buf []byte) []byte {
	buf1 := buf
	ls.height = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.startLevel = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.denseNodeCount = endian.Uint32(buf1)
	buf1 = buf1[4:]
	ls.denseChildCount = endian.Uint32(buf1)
	buf1 = buf1[4:]

	buf1 = ls.labelVec.Unmarshal(buf1)
	buf1 = ls.hasChildVec.Unmarshal(buf1)
	buf1 = ls.loudsVec.Unmarshal(buf1)
	buf1 = ls.suffixes.Unmarshal(buf1)
	buf1 = ls.prefixVec.Unmarshal(buf1)

	sz := align(int64(len(buf) - len(buf1)))
	return buf[sz:]
}

func (ls *loudsSparse) suffixPos(pos uint32) uint32 {
	return pos - ls.hasChildVec.Rank(pos)
}

func (ls *loudsSparse) firstLabelPos(nodeID uint32) uint32 {
	return ls.loudsVec.Select(nodeID + 1 - ls.denseNodeCount)
}

func (ls *loudsSparse) sparseLevels() uint32 {
	return ls.height - ls.startLevel
}
func (ls *loudsSparse) prefixID(nodeID uint32) uint32 {
	return nodeID - ls.denseNodeCount
}

func (ls *loudsSparse) lastLabelPos(nodeID uint32) uint32 {
	nextRank := nodeID + 2 - ls.denseNodeCount
	if nextRank > ls.loudsVec.numOnes {
		return ls.loudsVec.numBits - 1
	}
	return ls.loudsVec.Select(nextRank) - 1
}

func (ls *loudsSparse) childNodeID(pos uint32) uint32 {
	return ls.hasChildVec.Rank(pos) + ls.denseChildCount
}

func (ls *loudsSparse) nodeSize(pos uint32) uint32 {
	return ls.loudsVec.DistanceToNextSetBit(pos)
}

func (ls *loudsSparse) isEndOfNode(pos uint32) bool {
	return pos == ls.loudsVec.numBits-1 || ls.loudsVec.IsSet(pos+1)
}

type sparseIter struct {
	valid        bool
	atTerminator bool
	ls           *loudsSparse
	startLevel   uint32
	startNodeID  uint32
	startDepth   uint32
	level        uint32
	keyBuf       []byte
	posInTrie    []uint32
	nodeID       []uint32
	prefixLen    []uint32
}

func (it *sparseIter) Init(ls *loudsSparse) {
	it.ls = ls
	it.startLevel = ls.startLevel
	it.posInTrie = make([]uint32, ls.height-ls.startLevel)
	it.prefixLen = make([]uint32, ls.height-ls.startLevel)
	it.nodeID = make([]uint32, ls.height-ls.startLevel)
}

func (it *sparseIter) Next() {
	it.atTerminator = false
	pos := it.posInTrie[it.level] + 1
	nodeID := it.nodeID[it.level]

	for pos >= it.ls.loudsVec.numBits || it.ls.loudsVec.IsSet(pos) {
		if it.level == 0 {
			it.valid = false
			it.keyBuf = it.keyBuf[:0]
			return
		}
		it.level--
		pos = it.posInTrie[it.level] + 1
		nodeID = it.nodeID[it.level]
	}
	it.setAt(it.level, pos, nodeID)
	it.MoveToLeftMostKey()
}

func (it *sparseIter) Prev() {
	it.atTerminator = false
	pos := it.posInTrie[it.level]
	nodeID := it.nodeID[it.level]

	if pos == 0 {
		it.valid = false
		return
	}
	for it.ls.loudsVec.IsSet(pos) {
		if it.level == 0 {
			it.valid = false
			it.keyBuf = it.keyBuf[:0]
			return
		}
		it.level--
		pos = it.posInTrie[it.level]
		nodeID = it.nodeID[it.level]
	}
	it.setAt(it.level, pos-1, nodeID)
	it.MoveToRightMostKey()
}

func (it *sparseIter) Seek(key []byte) bool {
	nodeID := it.startNodeID
	pos := it.ls.firstLabelPos(nodeID)
	var ok bool
	depth := it.startDepth

	for it.level = 0; it.level < it.ls.sparseLevels(); it.level++ {
		prefix := it.ls.prefixVec.GetPrefix(it.ls.prefixID(nodeID))
		var prefixCmp int
		if len(prefix) != 0 {
			end := int(depth) + len(prefix)
			if end > len(key) {
				end = len(key)
			}
			prefixCmp = bytes.Compare(prefix, key[depth:end])
		}

		if prefixCmp < 0 {
			if it.level == 0 {
				it.valid = false
				return false
			}
			it.level--
			it.Next()
			return false
		}

		depth += uint32(len(prefix))
		if depth >= uint32(len(key)) || prefixCmp > 0 {
			it.append(it.ls.labelVec.GetLabel(pos), pos, nodeID)
			it.MoveToLeftMostKey()
			return false
		}

		nodeSize := it.ls.nodeSize(pos)
		pos, ok = it.ls.labelVec.Search(key[depth], pos, nodeSize)
		if !ok {
			it.moveToLeftInNextSubTrie(pos, nodeID, nodeSize, key[depth])
			return false
		}

		it.append(key[depth], pos, nodeID)

		if !it.ls.hasChildVec.IsSet(pos) {
			return it.compareSuffixGreaterThan(key, pos, depth+1)
		}

		nodeID = it.ls.childNodeID(pos)
		pos = it.ls.firstLabelPos(nodeID)
		depth++
	}

	if it.ls.labelVec.GetLabel(pos) == labelTerminator && !it.ls.hasChildVec.IsSet(pos) && !it.ls.isEndOfNode(pos) {
		it.append(labelTerminator, pos, nodeID)
		it.atTerminator = true
		it.valid = true
		return false
	}

	if uint32(len(key)) <= depth {
		it.MoveToLeftMostKey()
		return false
	}

	it.valid = true
	return true
}

func (it *sparseIter) Key() []byte {
	if it.atTerminator {
		return it.keyBuf[:len(it.keyBuf)-1]
	}
	return it.keyBuf
}

func (it *sparseIter) Value() []byte {
	valPos := it.ls.suffixPos(it.posInTrie[it.level])
	return it.ls.values.Get(valPos)
}

func (it *sparseIter) Compare(key []byte) int {
	itKey := it.Key()
	startDepth := int(it.startDepth)
	if startDepth > len(key) {
		panic("dense compare have bug")
	}
	if startDepth == len(key) {
		if len(itKey) == 0 {
			return 0
		}
		return 1
	}
	cmpKey := key[startDepth:]

	cmpLen := len(itKey)
	if cmpLen > len(cmpKey) {
		cmpLen = len(cmpKey)
	}
	cmp := bytes.Compare(itKey[:cmpLen], cmpKey[:cmpLen])
	if cmp != 0 {
		return cmp
	}
	if len(itKey) > len(cmpKey) {
		return 1
	}
	if len(itKey) == len(cmpKey) && it.atTerminator {
		return 0
	}
	suffixPos := it.ls.suffixPos(it.posInTrie[it.level])
	return it.ls.suffixes.Compare(key, suffixPos, uint32(len(itKey)+startDepth))
}

func (it *sparseIter) Reset() {
	it.valid = false
	it.level = 0
	it.atTerminator = false
	it.keyBuf = it.keyBuf[:0]
}

func (it *sparseIter) MoveToLeftMostKey() {
	if len(it.keyBuf) == 0 {
		pos := it.ls.firstLabelPos(it.startNodeID)
		label := it.ls.labelVec.GetLabel(pos)
		it.append(label, pos, it.startNodeID)
	}

	pos := it.posInTrie[it.level]
	label := it.ls.labelVec.GetLabel(pos)

	if !it.ls.hasChildVec.IsSet(pos) {
		if label == labelTerminator && !it.ls.isEndOfNode(pos) {
			it.atTerminator = true
		}
		it.valid = true
		return
	}

	for it.level < it.ls.sparseLevels() {
		it.level++
		nodeID := it.ls.childNodeID(pos)
		pos = it.ls.firstLabelPos(nodeID)
		label = it.ls.labelVec.GetLabel(pos)

		if !it.ls.hasChildVec.IsSet(pos) {
			it.append(label, pos, nodeID)
			if label == labelTerminator && !it.ls.isEndOfNode(pos) {
				it.atTerminator = true
			}
			it.valid = true
			return
		}
		it.append(label, pos, nodeID)
	}
	panic("unreachable")
}

func (it *sparseIter) MoveToRightMostKey() {
	if len(it.keyBuf) == 0 {
		pos := it.ls.lastLabelPos(it.startNodeID)
		label := it.ls.labelVec.GetLabel(pos)
		it.append(label, pos, it.startNodeID)
	}

	pos := it.posInTrie[it.level]
	label := it.ls.labelVec.GetLabel(pos)

	if !it.ls.hasChildVec.IsSet(pos) {
		if label == labelTerminator && !it.ls.isEndOfNode(pos) {
			it.atTerminator = true
		}
		it.valid = true
		return
	}

	for it.level < it.ls.sparseLevels() {
		it.level++
		nodeID := it.ls.childNodeID(pos)
		pos = it.ls.lastLabelPos(nodeID)
		label = it.ls.labelVec.GetLabel(pos)

		if !it.ls.hasChildVec.IsSet(pos) {
			it.append(label, pos, nodeID)
			if label == labelTerminator && !it.ls.isEndOfNode(pos) {
				it.atTerminator = true
			}
			it.valid = true
			return
		}
		it.append(label, pos, nodeID)
	}
	panic("unreachable")
}

func (it *sparseIter) SetToFirstInRoot() {
	it.append(it.ls.labelVec.GetLabel(0), 0, it.startNodeID)
}

func (it *sparseIter) SetToLastInRoot() {
	pos := it.ls.lastLabelPos(0)
	it.append(it.ls.labelVec.GetLabel(pos), pos, it.startNodeID)
}

func (it *sparseIter) append(label byte, pos, nodeID uint32) {
	prefix := it.ls.prefixVec.GetPrefix(it.ls.prefixID(nodeID))
	it.keyBuf = append(it.keyBuf, prefix...)
	it.keyBuf = append(it.keyBuf, label)
	it.posInTrie[it.level] = pos
	it.prefixLen[it.level] = uint32(len(prefix)) + 1
	if it.level != 0 {
		it.prefixLen[it.level] += it.prefixLen[it.level-1]
	}
	it.nodeID[it.level] = nodeID
}

func (it *sparseIter) setAt(level, pos, nodeID uint32) {
	it.keyBuf = append(it.keyBuf[:it.prefixLen[level]-1], it.ls.labelVec.GetLabel(pos))
	it.posInTrie[it.level] = pos
}

func (it *sparseIter) truncate(level uint32) {
	it.keyBuf = it.keyBuf[:it.prefixLen[level]]
}

func (it *sparseIter) moveToLeftInNextSubTrie(pos, nodeID, nodeSize uint32, label byte) {
	pos, ok := it.ls.labelVec.SearchGreaterThan(label, pos, nodeSize)
	it.append(it.ls.labelVec.GetLabel(pos), pos, nodeID)
	if ok {
		it.MoveToLeftMostKey()
	} else {
		it.Next()
	}
}

func (it *sparseIter) compareSuffixGreaterThan(key []byte, pos, level uint32) bool {
	cmp := it.ls.suffixes.Compare(key, it.ls.suffixPos(pos), level)
	if cmp < 0 {
		it.Next()
		return false
	}
	it.valid = true
	return cmp == couldBePositive
}
