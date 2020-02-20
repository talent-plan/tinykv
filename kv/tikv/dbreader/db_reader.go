package dbreader

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type DBReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}

type RegionReader struct {
	txn    *badger.Txn
	region *metapb.Region
}

func NewRegionReader(txn *badger.Txn, region metapb.Region) *RegionReader {
	return &RegionReader{
		txn:    txn,
		region: &region,
	}
}

func (r *RegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	if err := util.CheckKeyInRegion(key, r.region); err != nil {
		return nil, err
	}
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *RegionReader) IterCF(cf string) engine_util.DBIterator {
	return NewRegionIterator(engine_util.NewCFIterator(cf, r.txn), r.region)
}

func (r *RegionReader) Close() {
	r.txn.Discard()
}

type BadgerReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(b.txn, cf, key)
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard()
}

// RegionIterator wraps a db iterator and only allow it to iterate in the region. It behaves as if underlying
// db only contains one region.
type RegionIterator struct {
	iter   *engine_util.BadgerIterator
	region *metapb.Region
}

func NewRegionIterator(iter *engine_util.BadgerIterator, region *metapb.Region) *RegionIterator {
	return &RegionIterator{
		iter:   iter,
		region: region,
	}
}

func (it *RegionIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *RegionIterator) Valid() bool {
	if !it.iter.Valid() || engine_util.ExceedEndKey(it.iter.Item().Key(), it.region.EndKey) {
		return false
	}
	return true
}

func (it *RegionIterator) ValidForPrefix(prefix []byte) bool {
	if !it.iter.ValidForPrefix(prefix) || engine_util.ExceedEndKey(it.iter.Item().Key(), it.region.EndKey) {
		return false
	}
	return true
}

func (it *RegionIterator) Close() {
	it.iter.Close()
}

func (it *RegionIterator) Next() {
	it.iter.Next()
}

func (it *RegionIterator) Seek(key []byte) {
	if err := util.CheckKeyInRegion(key, it.region); err != nil {
		panic(err)
	}
	it.iter.Seek(key)
}

func (it *RegionIterator) Rewind() {
	it.iter.Rewind()
}
