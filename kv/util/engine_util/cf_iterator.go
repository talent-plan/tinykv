package engine_util

import (
	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/jmhodges/levigo"
	"github.com/tecbot/gorocksdb"
)

type LdbItem struct {
	key       []byte
	value     []byte
	prefixLen int
}

func (i *LdbItem) Key() []byte {
	return i.key[i.prefixLen:]
}

func (i *LdbItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, i.key[i.prefixLen:])
}

func (i *LdbItem) Value() ([]byte, error) {
	return i.value, nil
}

func (i *LdbItem) ValueSize() int {
	return len(i.value)
}

func (i *LdbItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, i.value), nil
}

type LdbIterator struct {
	iter   *levigo.Iterator
	prefix string
}

type RdbIterator struct {
	iter   *gorocksdb.Iterator
	prefix string
}

func NewLDBIterator(cf string, db *levigo.DB, roptions *levigo.ReadOptions) *LdbIterator {
	return &LdbIterator{
		iter:   db.NewIterator(roptions),
		prefix: cf + "_",
	}
}

func (it *LdbIterator) Item() DBItem {
	return &LdbItem{
		key:       it.iter.Key(),
		value:     it.iter.Value(),
		prefixLen: len(it.prefix),
	}
}

func (it *LdbIterator) Valid() bool { return it.iter.Valid() }

// func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
// 	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
// }

func (it *LdbIterator) Close() {
	it.iter.Close()
}

func (it *LdbIterator) Next() {
	it.iter.Next()
}

func (it *LdbIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
	// it.iter.Seek(append([]byte(it.prefix), key...))
}

// func (it *LdbIterator) Rewind() {
// 	it.iter.Rewind()
// }

func NewRDBIterator(cf string, db *gorocksdb.DB, roptions *gorocksdb.ReadOptions) *RdbIterator {
	return &RdbIterator{
		iter:   db.NewIterator(roptions),
		prefix: cf + "_",
	}
}

func (it *RdbIterator) Item() DBItem {
	return &LdbItem{
		key:       it.iter.Key().Data(),
		value:     it.iter.Value().Data(),
		prefixLen: len(it.prefix),
	}
}

func (it *RdbIterator) Valid() bool { return it.iter.Valid() }

// func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
// 	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
// }

func (it *RdbIterator) Close() {
	it.iter.Close()
}

func (it *RdbIterator) Next() {
	it.iter.Next()
}

func (it *RdbIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
	// it.iter.Seek(append([]byte(it.prefix), key...))
}

type DBIterator interface {
	// Item returns pointer to the current key-value pair.
	Item() DBItem
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. Always check it.Valid() after a Next()
	// to ensure you have access to a valid it.Item().
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	Seek([]byte)

	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy returns a copy of the key of the item, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	Value() ([]byte, error)
	// ValueSize returns the size of the value.
	ValueSize() int
	// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	ValueCopy(dst []byte) ([]byte, error)
}

type CFItem struct {
	item      *badger.Item
	prefixLen int
}

// String returns a string representation of Item
func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)[i.prefixLen:]
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

type BadgerIterator struct {
	iter   *badger.Iterator
	prefix string
}

func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
		prefix: cf + "_",
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		item:      it.iter.Item(),
		prefixLen: len(it.prefix),
	}
}

func (it *BadgerIterator) Valid() bool { return it.iter.ValidForPrefix([]byte(it.prefix)) }

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Rewind() {
	it.iter.Rewind()
}
