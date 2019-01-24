package rocksdb

type blockIterator struct {
	data    []byte
	cursor  int
	invalid bool

	keyBuf   []byte
	valueBuf []byte
}

func newBlockIterator(block []byte) *blockIterator {
	it := new(blockIterator)
	it.Reset(block)
	return it
}

func (it *blockIterator) SeekToFirst() {
	it.Rewind()
	it.Next()
}

func (it *blockIterator) Rewind() {
	it.cursor = 0
}

func (it *blockIterator) Next() {
	if it.end() {
		it.invalid = true
		return
	}

	var prefixLen, keyLen, valueLen uint32
	var n int

	if prefixLen, n = decodeVarint32(it.currData()); n <= 0 {
		it.invalid = true
		return
	}
	it.cursor += n

	if keyLen, n = decodeVarint32(it.currData()); n <= 0 {
		it.invalid = true
		return
	}
	it.cursor += n

	if valueLen, n = decodeVarint32(it.currData()); n <= 0 {
		it.invalid = true
		return
	}
	it.cursor += n

	it.keyBuf = append(it.keyBuf[:prefixLen], it.currData()[:keyLen]...)
	it.cursor += int(keyLen)

	it.valueBuf = append(it.valueBuf[:0], it.currData()[:valueLen]...)
	it.cursor += int(valueLen)
}

func (it *blockIterator) Key() []byte {
	return it.keyBuf
}

func (it *blockIterator) Value() []byte {
	return it.valueBuf
}

func (it *blockIterator) Valid() bool {
	return !it.invalid
}

func (it *blockIterator) Reset(block []byte) {
	numRestarts := rocksEndian.Uint32(block[len(block)-4:])
	restartsSz := int(numRestarts*4 + 4)
	data := block[:len(block)-restartsSz]

	it.data = data
	it.cursor = 0
	it.invalid = false
	it.keyBuf = it.keyBuf[:0]
	it.valueBuf = it.valueBuf[:0]
}

func (it *blockIterator) currData() []byte {
	return it.data[it.cursor:]
}

func (it *blockIterator) end() bool {
	return it.cursor == len(it.data)
}
