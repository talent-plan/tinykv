package raftstore

import (
	"github.com/coocood/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

type Engines struct {
	kv       *badger.DB
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return en.write(en.kv, wb)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return en.write(en.raft, wb)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) write(db *badger.DB, wb *WriteBatch) error {
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

type WriteBatch struct {
	entries []*badger.Entry
}

func (wb *WriteBatch) Set(key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
}

func (wb *WriteBatch) SetMsg(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}
