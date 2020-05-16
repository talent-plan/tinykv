package storage

import (
	"bytes"
	"fmt"

	"github.com/Connor1996/badger/y"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// MemStorage is an in-memory storage engine used for testing. Data is not written to disk, nor sent to other
// nodes. It is intended for testing only.
type MemStorage struct {
	CfDefault *llrb.LLRB
	CfLock    *llrb.LLRB
	CfWrite   *llrb.LLRB
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		CfDefault: llrb.New(),
		CfLock:    llrb.New(),
		CfWrite:   llrb.New(),
	}
}

func (s *MemStorage) Start() error {
	return nil
}

func (s *MemStorage) Stop() error {
	return nil
}

func (s *MemStorage) Reader(ctx *kvrpcpb.Context) (StorageReader, error) {
	return &memReader{s, 0}, nil
}

func (s *MemStorage) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := memItem{data.Key, data.Value, false}
			switch data.Cf {
			case engine_util.CfDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case engine_util.CfLock:
				s.CfLock.ReplaceOrInsert(item)
			case engine_util.CfWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Delete:
			item := memItem{key: data.Key}
			switch data.Cf {
			case engine_util.CfDefault:
				s.CfDefault.Delete(item)
			case engine_util.CfLock:
				s.CfLock.Delete(item)
			case engine_util.CfWrite:
				s.CfWrite.Delete(item)
			}
		}
	}

	return nil
}

func (s *MemStorage) Get(cf string, key []byte) []byte {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case engine_util.CfDefault:
		result = s.CfDefault.Get(item)
	case engine_util.CfLock:
		result = s.CfLock.Get(item)
	case engine_util.CfWrite:
		result = s.CfWrite.Get(item)
	}

	if result == nil {
		return nil
	}

	return result.(memItem).value
}

func (s *MemStorage) Set(cf string, key []byte, value []byte) {
	item := memItem{key, value, true}
	switch cf {
	case engine_util.CfDefault:
		s.CfDefault.ReplaceOrInsert(item)
	case engine_util.CfLock:
		s.CfLock.ReplaceOrInsert(item)
	case engine_util.CfWrite:
		s.CfWrite.ReplaceOrInsert(item)
	}
}

func (s *MemStorage) HasChanged(cf string, key []byte) bool {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case engine_util.CfDefault:
		result = s.CfDefault.Get(item)
	case engine_util.CfLock:
		result = s.CfLock.Get(item)
	case engine_util.CfWrite:
		result = s.CfWrite.Get(item)
	}
	if result == nil {
		return true
	}

	return !result.(memItem).fresh
}

func (s *MemStorage) Len(cf string) int {
	switch cf {
	case engine_util.CfDefault:
		return s.CfDefault.Len()
	case engine_util.CfLock:
		return s.CfLock.Len()
	case engine_util.CfWrite:
		return s.CfWrite.Len()
	}

	return -1
}

// memReader is a StorageReader which reads from a MemStorage.
type memReader struct {
	inner     *MemStorage
	iterCount int
}

func (mr *memReader) GetCF(cf string, key []byte) ([]byte, error) {
	item := memItem{key: key}
	var result llrb.Item
	switch cf {
	case engine_util.CfDefault:
		result = mr.inner.CfDefault.Get(item)
	case engine_util.CfLock:
		result = mr.inner.CfLock.Get(item)
	case engine_util.CfWrite:
		result = mr.inner.CfWrite.Get(item)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}

	if result == nil {
		return nil, nil
	}

	return result.(memItem).value, nil
}

func (mr *memReader) IterCF(cf string) engine_util.DBIterator {
	var data *llrb.LLRB
	switch cf {
	case engine_util.CfDefault:
		data = mr.inner.CfDefault
	case engine_util.CfLock:
		data = mr.inner.CfLock
	case engine_util.CfWrite:
		data = mr.inner.CfWrite
	default:
		return nil
	}

	// 怎么理解这个？
	mr.iterCount += 1
	min := data.Min()
	if min == nil {
		return &memIter{data, memItem{}, mr}
	}
	return &memIter{data, min.(memItem), mr}
}

func (r *memReader) Close() {
	if r.iterCount > 0 {
		panic("Unclosed iterator")
	}
}

// 链表
type memIter struct {
	data   *llrb.LLRB
	item   memItem
	reader *memReader
}

func (it *memIter) Item() engine_util.DBItem {
	return it.item
}
func (it *memIter) Valid() bool {
	return it.item.key != nil
}
func (it *memIter) Next() {
	first := true
	oldItem := it.item
	it.item = memItem{}
	it.data.AscendGreaterOrEqual(oldItem, func(item llrb.Item) bool {
		// Skip the first item, which will be it.item
		if first {
			first = false
			return true
		}

		it.item = item.(memItem)
		return false
	})
}
func (it *memIter) Seek(key []byte) {
	it.item = memItem{}
	it.data.AscendGreaterOrEqual(memItem{key: key}, func(item llrb.Item) bool {
		it.item = item.(memItem)

		return false
	})
}

func (it *memIter) Close() {
	it.reader.iterCount -= 1
}

type memItem struct {
	key   []byte
	value []byte
	fresh bool
}

func (it memItem) Key() []byte {
	return it.key
}

func (it memItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it memItem) Value() ([]byte, error) {
	return it.value, nil
}
func (it memItem) ValueSize() int {
	return len(it.value)
}
func (it memItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}

func (it memItem) Less(than llrb.Item) bool {
	other := than.(memItem)
	return bytes.Compare(it.key, other.key) < 0
}
