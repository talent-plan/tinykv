package surf

import (
	"bytes"
	"io"
)

type SuRF struct {
	ld loudsDense
	ls loudsSparse
}

// Get returns the values mapped by the key, may return value for keys doesn't in SuRF.
func (s *SuRF) Get(key []byte) ([]byte, bool) {
	cont, depth, value, ok := s.ld.Get(key)
	if !ok || cont < 0 {
		return value, ok
	}
	return s.ls.Get(key, depth, uint32(cont))
}

// HasOverlap returns does SuRF overlap with [start, end].
func (s *SuRF) HasOverlap(start, end []byte, includeEnd bool) bool {
	if s.ld.height == 0 && s.ls.height == 0 {
		return false
	}
	it := s.NewIterator()
	it.Seek(start)
	if !it.Valid() {
		return false
	}

	cmp := it.compare(end)
	if cmp == couldBePositive {
		cmp = -1
	}
	if includeEnd {
		return cmp <= 0
	}
	return cmp < 0
}

// MarshalSize returns the size of SuRF after serialization.
func (s *SuRF) MarshalSize() int64 {
	return s.ld.MarshalSize() + s.ls.MarshalSize() + s.ld.values.MarshalSize() + s.ls.values.MarshalSize()
}

// Marshal returns the serialized SuRF.
func (s *SuRF) Marshal() []byte {
	w := bytes.NewBuffer(make([]byte, 0, s.MarshalSize()))
	_ = s.WriteTo(w)
	return w.Bytes()
}

// WriteTo serialize SuRF to writer.
func (s *SuRF) WriteTo(w io.Writer) error {
	if err := s.ld.WriteTo(w); err != nil {
		return err
	}
	if err := s.ls.WriteTo(w); err != nil {
		return err
	}
	if err := s.ld.values.WriteTo(w); err != nil {
		return err
	}
	if err := s.ls.values.WriteTo(w); err != nil {
		return err
	}
	return nil
}

// Unmarshal deserialize SuRF from bytes.
func (s *SuRF) Unmarshal(b []byte) {
	b = s.ld.Unmarshal(b)
	b = s.ls.Unmarshal(b)
	b = s.ld.values.Unmarshal(b)
	s.ls.values.Unmarshal(b)
}

// Iterator is iterator of SuRF.
type Iterator struct {
	denseIter  denseIter
	sparseIter sparseIter
	keyBuf     []byte
}

// NewIterator returns a new SuRF iterator.
func (s *SuRF) NewIterator() *Iterator {
	iter := new(Iterator)
	iter.denseIter.Init(&s.ld)
	iter.sparseIter.Init(&s.ls)
	return iter
}

// Valid returns the valid status of iterator.
func (it *Iterator) Valid() bool {
	if it.denseIter.ld.height == 0 {
		return it.sparseIter.valid
	}
	return it.denseIter.valid && (it.denseIter.IsComplete() || it.sparseIter.valid)
}

// Next move the iterator to next key.
func (it *Iterator) Next() {
	if it.incrSparseIter() {
		return
	}
	it.incrDenseIter()
}

// Prev move the iterator to previous key.
func (it *Iterator) Prev() {
	if it.decrSparseIter() {
		return
	}
	it.decrDenseIter()
}

// Seek move the iterator to the first greater or equals to key.
func (it *Iterator) Seek(key []byte) bool {
	var fp bool
	it.Reset()

	if it.sparseIter.ls.height == 0 && it.denseIter.ld.height == 0 {
		return false
	}

	fp = it.denseIter.Seek(key)
	if !it.denseIter.valid || it.denseIter.IsComplete() {
		return fp
	}

	if !it.denseIter.searchComp {
		it.passToSparse()
		fp = it.sparseIter.Seek(key)
		if !it.sparseIter.valid {
			it.incrDenseIter()
		}
		return fp
	} else if !it.denseIter.leftComp {
		it.passToSparse()
		it.sparseIter.MoveToLeftMostKey()
		return fp
	}

	panic("invalid state")
}

// SeekToFirst move the iterator to the first key in SuRF.
func (it *Iterator) SeekToFirst() {
	it.Reset()
	if it.denseIter.ld.height > 0 {
		it.denseIter.SetToFirstInRoot()
		it.denseIter.MoveToLeftMostKey()
		if it.denseIter.leftComp {
			return
		}
		it.passToSparse()
		it.sparseIter.MoveToLeftMostKey()
	} else if it.sparseIter.ls.height > 0 {
		it.sparseIter.SetToFirstInRoot()
		it.sparseIter.MoveToLeftMostKey()
	}
}

// SeekToLast move the iterator to the last key in SuRF.
func (it *Iterator) SeekToLast() {
	it.Reset()
	if it.denseIter.ld.height > 0 {
		it.denseIter.SetToLastInRoot()
		it.denseIter.MoveToRightMostKey()
		if it.denseIter.rightComp {
			return
		}
		it.passToSparse()
		it.sparseIter.MoveToRightMostKey()
	} else if it.sparseIter.ls.height > 0 {
		it.sparseIter.SetToLastInRoot()
		it.sparseIter.MoveToRightMostKey()
	}
}

// Key returns the key where the iterator at.
func (it *Iterator) Key() []byte {
	if it.denseIter.IsComplete() {
		return it.denseIter.Key()
	}
	it.keyBuf = append(it.keyBuf[:0], it.denseIter.Key()...)
	return append(it.keyBuf, it.sparseIter.Key()...)
}

// Value returns the value where the iterator at.
func (it *Iterator) Value() []byte {
	if it.denseIter.IsComplete() {
		return it.denseIter.Value()
	}
	return it.sparseIter.Value()
}

// Reset rest iterator's states and buffers.
func (it *Iterator) Reset() {
	it.denseIter.Reset()
	it.sparseIter.Reset()
}

func (it *Iterator) passToSparse() {
	it.sparseIter.startNodeID = it.denseIter.sendOutNodeID
	it.sparseIter.startDepth = it.denseIter.sendOutDepth
}

func (it *Iterator) incrDenseIter() bool {
	if !it.denseIter.valid {
		return false
	}

	it.denseIter.Next()
	if !it.denseIter.valid {
		return false
	}
	if it.denseIter.leftComp {
		return true
	}

	it.passToSparse()
	it.sparseIter.MoveToLeftMostKey()
	return true
}

func (it *Iterator) incrSparseIter() bool {
	if !it.sparseIter.valid {
		return false
	}
	it.sparseIter.Next()
	return it.sparseIter.valid
}

func (it *Iterator) decrDenseIter() bool {
	if !it.denseIter.valid {
		return false
	}

	it.denseIter.Prev()
	if !it.denseIter.valid {
		return false
	}
	if it.denseIter.rightComp {
		return true
	}

	it.passToSparse()
	it.sparseIter.MoveToRightMostKey()
	return true
}

func (it *Iterator) decrSparseIter() bool {
	if !it.sparseIter.valid {
		return false
	}
	it.sparseIter.Prev()
	return it.sparseIter.valid
}

func (it *Iterator) compare(key []byte) int {
	cmp := it.denseIter.Compare(key)
	if it.denseIter.IsComplete() || cmp != 0 {
		return cmp
	}
	return it.sparseIter.Compare(key)
}
