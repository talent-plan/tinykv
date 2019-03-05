package raftstore

import (
	"github.com/coocood/badger"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/lockstore"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type DBBundle struct {
	db            *badger.DB
	lockStore     *lockstore.MemStore
	rollbackStore *lockstore.MemStore
}

type DBSnapshot struct {
	Txn *badger.Txn
	LockStore *lockstore.MemStore
	RollbackStore *lockstore.MemStore
}

func NewDBSnapshot(db *DBBundle) *DBSnapshot {
	return &DBSnapshot{
		Txn: db.db.NewTransaction(false),
		LockStore: db.lockStore,
		RollbackStore: db.rollbackStore,
	}
}

type RegionSnapshot struct {
	Region *metapb.Region
	Snapshot *DBSnapshot
}

type Engines struct {
	kv       *badger.DB
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.raft)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

type WriteBatch struct {
	entries []*badger.Entry
	size    int
}

func (wb *WriteBatch) Set(key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
}

func (wb *WriteBatch) SetWithUserMeta(key, val, useMeta []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: useMeta,
	})
	wb.size += len(key) + len(val) + len(useMeta)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetMsg(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}

func (wb *WriteBatch) WriteToDB(db *badger.DB) error {
	if len(wb.entries) == 0 {
		return nil
	}
	err := db.Update(func(txn *badger.Txn) error {
		var err1 error
		for _, entry := range wb.entries {
			if len(entry.Value) == 0 {
				err1 = txn.Delete(entry.Key)
			} else {
				err1 = txn.SetEntry(entry)
			}
			if err1 != nil {
				return err1
			}
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
