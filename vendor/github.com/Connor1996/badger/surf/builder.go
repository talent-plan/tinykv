package surf

// Builder is builder of SuRF.
type Builder struct {
	sparseStartLevel uint32
	valueSize        uint32
	totalCount       int

	// LOUDS-Sparse bitvecs
	lsLabels    [][]byte
	lsHasChild  [][]uint64
	lsLoudsBits [][]uint64

	// LOUDS-Dense bitvecs
	ldLabels   [][]uint64
	ldHasChild [][]uint64
	ldIsPrefix [][]uint64

	// suffix
	hashSuffixLen uint32
	realSuffixLen uint32
	suffixes      [][]uint64
	suffixCounts  []uint32

	// value
	values      [][]byte
	valueCounts []uint32

	// prefix
	hasPrefix [][]uint64
	prefixes  [][][]byte

	nodeCounts           []uint32
	isLastItemTerminator []bool
}

// NewBuilder returns a new SuRF builder.
func NewBuilder(valueSize uint32, hashSuffixLen, realSuffixLen uint32) *Builder {
	return &Builder{
		valueSize:     valueSize,
		hashSuffixLen: hashSuffixLen,
		realSuffixLen: realSuffixLen,
	}
}

// Build returns the SuRF for added kv pairs.
// The bitsPerKeyHint is a size hint used when determine how many levels can use the dense-loudes format.
// The dense-loudes format is faster than sparse-loudes format, but may consume more space.
func (b *Builder) Build(keys, vals [][]byte, bitsPerKeyHint int) *SuRF {
	b.totalCount = len(keys)
	b.buildNodes(keys, vals, 0, 0, 0)
	b.determineCutoffLevel(bitsPerKeyHint)
	b.buildDense()

	surf := new(SuRF)
	surf.ld.Init(b)
	surf.ls.Init(b)
	return surf
}

// buildNodes is recursive algorithm to bulk building SuRF nodes.
//	* We divide keys into groups by the `key[depth]`, so keys in each group shares the same prefix
//	* If depth larger than the length if the first key in group, the key is prefix of others in group
//	  So we should append `labelTerminator` to labels and update `b.isLastItemTerminator`, then remove it from group.
//	* Scan over keys in current group when meets different label, use the new sub group call buildNodes with level+1 recursively
//	* If all keys in current group have the same label, this node can be compressed, use this group call buildNodes with level recursively.
//	* If current group contains only one key constract suffix of this key and return.
func (b *Builder) buildNodes(keys, vals [][]byte, prefixDepth, depth, level int) {
	b.ensureLevel(level)
	nodeStartPos := b.numItems(level)

	groupStart := 0
	if depth >= len(keys[groupStart]) {
		b.lsLabels[level] = append(b.lsLabels[level], labelTerminator)
		b.isLastItemTerminator[level] = true
		b.insertSuffix(keys[groupStart], level, depth)
		b.insertValue(vals[groupStart], level)
		b.moveToNextItemSlot(level)
		groupStart++
	}

	for groupEnd := groupStart; groupEnd <= len(keys); groupEnd++ {
		if groupEnd < len(keys) && keys[groupStart][depth] == keys[groupEnd][depth] {
			continue
		}

		if groupEnd == len(keys) && groupStart == 0 && groupEnd-groupStart != 1 {
			// node at this level is one-way node, compress it to next node
			b.buildNodes(keys, vals, prefixDepth, depth+1, level)
			return
		}

		b.lsLabels[level] = append(b.lsLabels[level], keys[groupStart][depth])
		b.moveToNextItemSlot(level)
		if groupEnd-groupStart == 1 {
			b.insertSuffix(keys[groupStart], level, depth)
			b.insertValue(vals[groupStart], level)
		} else {
			setBit(b.lsHasChild[level], b.numItems(level)-1)
			b.buildNodes(keys[groupStart:groupEnd], vals[groupStart:groupEnd], depth+1, depth+1, level+1)
		}

		groupStart = groupEnd
	}

	// check if current node contains compressed path.
	if depth-prefixDepth > 0 {
		prefix := keys[0][prefixDepth:depth]
		setBit(b.hasPrefix[level], b.nodeCounts[level])
		b.insertPrefix(prefix, level)
	}
	setBit(b.lsLoudsBits[level], nodeStartPos)

	b.nodeCounts[level]++
	if b.nodeCounts[level]%wordSize == 0 {
		b.hasPrefix[level] = append(b.hasPrefix[level], 0)
	}
}

func (b *Builder) buildDense() {
	var level int
	for level = 0; uint32(level) < b.sparseStartLevel; level++ {
		b.initDenseVectors(level)
		if b.numItems(level) == 0 {
			continue
		}

		var nodeID uint32
		if b.isTerminator(level, 0) {
			setBit(b.ldIsPrefix[level], 0)
		} else {
			b.setLabelAndHasChildVec(level, nodeID, 0)
		}

		var pos uint32
		numItems := b.numItems(level)
		for pos = 1; pos < numItems; pos++ {
			if b.isStartOfNode(level, pos) {
				nodeID++
				if b.isTerminator(level, pos) {
					setBit(b.ldIsPrefix[level], nodeID)
					continue
				}
			}
			b.setLabelAndHasChildVec(level, nodeID, pos)
		}
	}
}

func (b *Builder) ensureLevel(level int) {
	if level >= b.treeHeight() {
		b.addLevel()
	}
}

func (b *Builder) suffixLen() uint32 {
	return b.hashSuffixLen + b.realSuffixLen
}

func (b *Builder) treeHeight() int {
	return len(b.nodeCounts)
}

func (b *Builder) numItems(level int) uint32 {
	return uint32(len(b.lsLabels[level]))
}

func (b *Builder) addLevel() {
	b.lsLabels = append(b.lsLabels, []byte{})
	b.lsHasChild = append(b.lsHasChild, []uint64{})
	b.lsLoudsBits = append(b.lsLoudsBits, []uint64{})
	b.hasPrefix = append(b.hasPrefix, []uint64{})
	b.suffixes = append(b.suffixes, []uint64{})
	b.suffixCounts = append(b.suffixCounts, 0)
	b.values = append(b.values, []byte{})
	b.valueCounts = append(b.valueCounts, 0)
	b.prefixes = append(b.prefixes, [][]byte{})

	b.nodeCounts = append(b.nodeCounts, 0)
	b.isLastItemTerminator = append(b.isLastItemTerminator, false)

	level := b.treeHeight() - 1
	b.lsHasChild[level] = append(b.lsHasChild[level], 0)
	b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
	b.hasPrefix[level] = append(b.hasPrefix[level], 0)
}

func (b *Builder) moveToNextItemSlot(level int) {
	if b.numItems(level)%wordSize == 0 {
		b.lsHasChild[level] = append(b.lsHasChild[level], 0)
		b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
	}
}

func (b *Builder) insertSuffix(key []byte, level, depth int) {
	if level >= b.treeHeight() {
		b.addLevel()
	}
	suffix := constructSuffix(key, uint32(depth)+1, b.realSuffixLen, b.hashSuffixLen)

	suffixLen := b.suffixLen()
	pos := b.suffixCounts[level] * suffixLen
	if pos == uint32(len(b.suffixes[level])*wordSize) {
		b.suffixes[level] = append(b.suffixes[level], 0)
	}
	wordID := pos / wordSize
	offset := pos % wordSize
	remain := wordSize - offset
	b.suffixes[level][wordID] |= suffix << offset
	if suffixLen > remain {
		b.suffixes[level] = append(b.suffixes[level], suffix>>remain)
	}
	b.suffixCounts[level]++
}

func (b *Builder) insertValue(value []byte, level int) {
	b.values[level] = append(b.values[level], value[:b.valueSize]...)
	b.valueCounts[level]++
}

func (b *Builder) insertPrefix(prefix []byte, level int) {
	b.prefixes[level] = append(b.prefixes[level], append([]byte{}, prefix...))
}

func (b *Builder) determineCutoffLevel(bitsPerKeyHint int) {
	height := b.treeHeight()
	if height == 0 {
		return
	}

	sizeHint := uint64(b.totalCount * bitsPerKeyHint)
	suffixSize := uint64(b.totalCount) * uint64(b.suffixLen())
	var prefixSize uint64
	for _, l := range b.prefixes {
		for _, p := range l {
			prefixSize += uint64(len(p)) * 8
		}
	}
	for _, nc := range b.nodeCounts {
		prefixSize += uint64(nc)
	}

	var level int
	// Begins from last level to make the height of dense levels as large as possible.
	for level = height - 1; level > 0; level-- {
		ds := b.denseSizeNoSuffix(level)
		ss := b.sparseSizeNoSuffix(level)
		sz := ds + ss + suffixSize + prefixSize
		if sz <= sizeHint {
			break
		}
	}
	b.sparseStartLevel = uint32(level)
}

func (b *Builder) denseSizeNoSuffix(level int) uint64 {
	var total uint64
	for l := 0; l < level; l++ {
		total += uint64(2 * denseFanout * b.nodeCounts[l])
		if l > 0 {
			total += uint64(b.nodeCounts[l-1])
		}
	}
	return total
}

func (b *Builder) sparseSizeNoSuffix(level int) uint64 {
	var total uint64
	height := b.treeHeight()
	for l := level; l < height; l++ {
		n := uint64(len(b.lsLabels[l]))
		total += n*8 + 2*n
	}
	return total
}

func (b *Builder) setLabelAndHasChildVec(level int, nodeID, pos uint32) {
	label := b.lsLabels[level][pos]
	setBit(b.ldLabels[level], nodeID*denseFanout+uint32(label))
	if readBit(b.lsHasChild[level], pos) {
		setBit(b.ldHasChild[level], nodeID*denseFanout+uint32(label))
	}
}

func (b *Builder) initDenseVectors(level int) {
	vecLength := b.nodeCounts[level] * (denseFanout / wordSize)
	prefixVecLen := b.nodeCounts[level] / wordSize
	if b.nodeCounts[level]%wordSize != 0 {
		prefixVecLen++
	}

	b.ldLabels = append(b.ldLabels, make([]uint64, vecLength))
	b.ldHasChild = append(b.ldHasChild, make([]uint64, vecLength))
	b.ldIsPrefix = append(b.ldIsPrefix, make([]uint64, prefixVecLen))
}

func (b *Builder) isStartOfNode(level int, pos uint32) bool {
	return readBit(b.lsLoudsBits[level], pos)
}

func (b *Builder) isTerminator(level int, pos uint32) bool {
	label := b.lsLabels[level][pos]
	return (label == labelTerminator) && !readBit(b.lsHasChild[level], pos)
}
