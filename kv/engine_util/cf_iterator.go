package engine_util

import (
	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
)

type CFItem struct {
	item      *badger.Item
	prefixLen int
}

// String returns a string representation of Item
func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen+1:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, i.Key())
}

func (i *CFItem) Version() uint64 {
	return i.item.Version()
}

func (i *CFItem) IsEmpty() bool {
	return i.item.IsEmpty()
}

func (i *CFItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *CFItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *CFItem) IsDeleted() bool {
	return i.item.IsDeleted()
}

func (i *CFItem) EstimatedSize() int64 {
	return i.item.EstimatedSize()
}

func (i *CFItem) UserMeta() []byte {
	return i.item.UserMeta()
}

type CFIterator struct {
	iter *badger.Iterator
	cf   string
}

func NewCFIterator(cf string, txn *badger.Txn) *CFIterator {
	return &CFIterator{
		iter: txn.NewIterator(badger.DefaultIteratorOptions),
		cf:   cf,
	}
}

func (it *CFIterator) Item() *CFItem {
	return &CFItem{
		item:      it.iter.Item(),
		prefixLen: len(it.cf),
	}
}

func (it *CFIterator) Valid() bool { return it.iter.ValidForPrefix([]byte(it.cf)) }

func (it *CFIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append(prefix, []byte(it.cf)...))
}

func (it *CFIterator) Close() {
	it.iter.Close()
}

func (it *CFIterator) Next() {
	it.iter.Next()
}

func (it *CFIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.cf+"_"), key...))
}

func (it *CFIterator) Rewind() {
	it.iter.Rewind()
}
