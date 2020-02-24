package snap

import (
	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

// snapBuilder builds snapshot files.
type snapBuilder struct {
	region  *metapb.Region
	txn     *badger.Txn
	cfFiles []*CFFile
	kvCount int
	size    int
}

func newSnapBuilder(cfFiles []*CFFile, dbSnap *badger.Txn, region *metapb.Region) *snapBuilder {
	return &snapBuilder{
		region:  region,
		cfFiles: cfFiles,
		txn:     dbSnap,
	}
}

func (b *snapBuilder) build() error {
	defer b.txn.Discard()
	startKey, endKey := b.region.StartKey, b.region.EndKey

	for _, file := range b.cfFiles {
		cf := file.CF
		sstWriter := file.SstWriter

		it := engine_util.NewCFIterator(cf, b.txn)
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if engine_util.ExceedEndKey(key, endKey) {
				break
			}
			value, err := item.Value()
			if err != nil {
				return err
			}
			cfKey := engine_util.KeyWithCF(cf, key)
			if err := sstWriter.Add(cfKey, y.ValueStruct{
				Value: value,
			}); err != nil {
				return err
			}
			file.KVCount++
			file.Size += uint64(len(cfKey) + len(value))
		}
		it.Close()
		b.kvCount += file.KVCount
		b.size += int(file.Size)
	}
	return nil
}
