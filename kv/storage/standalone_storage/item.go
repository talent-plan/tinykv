package standalone_storage

import (
	"github.com/Connor1996/badger/y"
)

type item struct {
	key   []byte
	value []byte

	fresh bool
}

func (it item) Key() []byte {
	return []byte(keyWithoutPrefix(string(it.key)))
}

func (it item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it item) Value() ([]byte, error) {
	return it.value, nil
}
func (it item) ValueSize() int {
	return len(it.value)
}
func (it item) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}
