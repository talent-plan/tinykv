package standalone_storage

import (
	"github.com/dgraph-io/badger"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type iterator struct {
	data   *badger.Item
	reader *reader
}

func (it *iterator) iterator() engine_util.DBItem {
	return nil
}

func (it *iterator) Valid() bool {
	return false
}

func (it *iterator) Next() {
	return
}

func (it *iterator) Seek(key []byte) {
	return
}

func (it *iterator) Close() {
	return
}
