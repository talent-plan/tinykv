package standalone_storage

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type iterator struct {
	collections []*item
	curIndex    int
	iterCount   int
}

func (it *iterator) iterator() engine_util.DBItem {
	cur := it.collections[it.curIndex]
	v, _ := cur.Value()
	return &item{
		key:   cur.Key(),
		value: v,
	}
}

func (it *iterator) Item() engine_util.DBItem {
	// debug fmt.Println("cur", it.curIndex)
	return it.collections[it.curIndex]
}

func (it *iterator) Valid() bool {
	if it.curIndex < it.iterCount &&
		len(it.collections) == it.iterCount &&
		it.curIndex != -1 {
		return true
	}
	return false
}

func (it *iterator) Next() {
	if it.curIndex < it.iterCount {
		it.curIndex++
	}
	// fmt.Println("Next cur:", it.curIndex)
	// fmt.Println(string(it.collections[it.curIndex].Key()))
	return
}

func (it *iterator) Seek(key []byte) {
	fmt.Println("seek")
	fmt.Println(it.curIndex)
	fmt.Println(len(it.collections))
	for i := it.curIndex; i < it.iterCount; i++ {
		fmt.Println("?")
		if string(it.Item().Key()) == string(key) {
			break
		}
		it.curIndex++
		fmt.Println(i)
	}
}

func (it *iterator) Close() {
	// how to free the iterator ?
	if it.curIndex == it.iterCount-1 {
		it.collections = nil
		it.iterCount = 0
		it.curIndex = -1
	}
	return
}
