package standalone_storage

import (
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type reader struct {
	storage   *StandAloneStorage
	iterCount int
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	realKey := createIndexKey(cf, string(key))
	// TODO:这样写有点 hack
	// 能不能优化一下？
	var ret []byte
	err := r.storage.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(realKey))
		if err != nil {
			return err
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		ret = v
		return nil
	})
	switch err {
	case nil:
	case badger.ErrKeyNotFound:
		return nil, badger.ErrKeyNotFound
	default:
		return nil, err
	}

	return ret, nil
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	var items []*item
	count := 0

	// we need deey copy here
	r.storage.badgerDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		cfKey := createCF(cf)
		// 又再次踩了一个坑
		for it.Seek([]byte(cfKey)); it.ValidForPrefix([]byte(cfKey)); it.Next() {
			// item := it.Item()
			v, _ := it.Item().Value()
			items = append(items, &item{
				key:   it.Item().Key(),
				value: v,
			})
			count++
			// for debug
			// fmt.Println(string(it.Item().Key()))
			// fmt.Println(count)
		}
		return nil
	})

	// debug code
	//fmt.Println("collections", len(items))
	//fmt.Println("count", count)
	//
	//fmt.Println("---")
	//for _, r := range items {
	//	fmt.Println(string(r.Key()))
	//}
	//fmt.Println("-----")

	return &iterator{
		collections: items,
		curIndex:    0,
		iterCount:   count,
	}
}

func (r *reader) Close() {
	return
}
