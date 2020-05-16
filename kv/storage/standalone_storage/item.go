package standalone_storage

import (
	"github.com/petar/GoLLRB/llrb"
)

type item struct {
	key   []byte
	value []byte
	fresh bool
}

func (it item) Key() []byte {
	return nil
}

func (it item) KeyCopy(dst []byte) []byte {
	return nil
}
func (it item) Value() ([]byte, error) {
	return nil, nil
}
func (it item) ValueSize() int {
	return 0
}
func (it item) ValueCopy(dst []byte) ([]byte, error) {
	return nil, nil
}

func (it item) Less(than llrb.Item) bool {
	return false
}
