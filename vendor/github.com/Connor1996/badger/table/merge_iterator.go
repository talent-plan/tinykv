package table

import (
	"github.com/Connor1996/badger/y"
)

// MergeTowIterator is a specialized MergeIterator that only merge tow iterators.
// It is an optimization for compaction.
type MergeIterator struct {
	smaller mergeIteratorChild
	bigger  mergeIteratorChild

	// when the two iterators has the same value, the value in the second iterator is ignored.
	second  y.Iterator
	reverse bool
}

type mergeIteratorChild struct {
	valid bool
	key   []byte
	iter  y.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (child *mergeIteratorChild) setIterator(iter y.Iterator) {
	child.iter = iter
	child.merge, _ = iter.(*MergeIterator)
	child.concat, _ = iter.(*ConcatIterator)
}

func (child *mergeIteratorChild) reset() {
	if child.merge != nil {
		child.valid = child.merge.smaller.valid
		if child.valid {
			child.key = child.merge.smaller.key
		}
	} else if child.concat != nil {
		child.valid = child.concat.Valid()
		if child.valid {
			child.key = child.concat.Key()
		}
	} else {
		child.valid = child.iter.Valid()
		if child.valid {
			child.key = child.iter.Key()
		}
	}
}

func (mt *MergeIterator) fix() {
	if !mt.bigger.valid {
		return
	}
	for mt.smaller.valid {
		cmp := y.CompareKeysWithVer(mt.smaller.key, mt.bigger.key)
		if cmp == 0 {
			// Ignore the value in second iterator.
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.smaller.iter {
				mt.smaller.reset()
				secondValid = mt.smaller.valid
			} else {
				mt.bigger.reset()
				secondValid = mt.bigger.valid
			}
			if !secondValid {
				if mt.second == mt.smaller.iter && mt.bigger.valid {
					mt.swap()
				}
				return
			}
			continue
		}
		if mt.reverse {
			if cmp < 0 {
				mt.swap()
			}
		} else {
			if cmp > 0 {
				mt.swap()
			}
		}
		return
	}
	mt.swap()
}

func (mt *MergeIterator) swap() {
	mt.smaller, mt.bigger = mt.bigger, mt.smaller
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	if mt.smaller.merge != nil {
		mt.smaller.merge.Next()
	} else if mt.smaller.concat != nil {
		mt.smaller.concat.Next()
	} else {
		mt.smaller.iter.Next()
	}
	mt.smaller.reset()
	mt.fix()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.smaller.iter.Rewind()
	mt.smaller.reset()
	mt.bigger.iter.Rewind()
	mt.bigger.reset()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.smaller.iter.Seek(key)
	mt.smaller.reset()
	mt.bigger.iter.Seek(key)
	mt.bigger.reset()
	mt.fix()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.smaller.valid
}

// Key returns the key associated with the current iterator
func (mt *MergeIterator) Key() []byte {
	return mt.smaller.key
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() y.ValueStruct {
	return mt.smaller.iter.Value()
}

func (mt *MergeIterator) FillValue(vs *y.ValueStruct) {
	if mt.smaller.merge != nil {
		mt.smaller.merge.FillValue(vs)
	} else if mt.smaller.concat != nil {
		mt.smaller.concat.FillValue(vs)
	} else {
		mt.smaller.iter.FillValue(vs)
	}
}


// NewMergeIterator creates a merge iterator
func NewMergeIterator(iters []y.Iterator, reverse bool) y.Iterator {
	if len(iters) == 0 {
		return &EmptyIterator{}
	} else if len(iters) == 1 {
		return iters[0]
	} else if len(iters) == 2 {
		mi := &MergeIterator{
			second:  iters[1],
			reverse: reverse,
		}
		mi.smaller.setIterator(iters[0])
		mi.bigger.setIterator(iters[1])
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator([]y.Iterator{NewMergeIterator(iters[:mid], reverse), NewMergeIterator(iters[mid:], reverse)}, reverse)
}

type EmptyIterator struct{}

func (e *EmptyIterator) Next() {}

func (e *EmptyIterator) Rewind() {}

func (e *EmptyIterator) Seek(key []byte) {}

func (e *EmptyIterator) Key() []byte {
	return nil
}

func (e *EmptyIterator) Value() y.ValueStruct {
	return y.ValueStruct{}
}

func (e *EmptyIterator) FillValue(vs *y.ValueStruct) {}

func (e *EmptyIterator) Valid() bool {
	return false
}
