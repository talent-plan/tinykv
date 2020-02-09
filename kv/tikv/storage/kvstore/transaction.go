package kvstore

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// MvccTxn represents an mvcc transaction (see tikv/storage/doc.go for a definition). It permits reading from a snapshot
// and stores writes in a buffer for atomic writing.
type MvccTxn struct {
	// TODO: is reader a snapshot or not?
	Reader  dbreader.DBReader
	Writes  []inner_server.Modify
	StartTS *uint64
}

func NewTxn(reader dbreader.DBReader) MvccTxn {
	return MvccTxn{
		reader,
		nil,
		nil,
	}
}

// SeekWrite returns the Write from the DB and the write's commit timestamp, or an error.
func (txn *MvccTxn) SeekWrite(key []byte) (*Write, uint64, error) {
	return nil, 0, nil
}

// FindWrittenValue searches for a put write at key and with a timestamp at ts or later, if it finds one, it uses the
// write's timestamp to look up the value.
func (txn *MvccTxn) FindWrittenValue(key []byte, ts uint64) ([]byte, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	bts := [8]byte{}
	binary.BigEndian.PutUint64(bts[:], ts)
	for iter.Seek(codec.EncodeKey(key, ts)); iter.Valid(); iter.Next() {
		item := iter.Item()
		// If the user key part of the combined key has changed, then we've got to the next key without finding a put write.
		if bytes.Compare(codec.DecodeUserKey(item.Key()), key) != 0 {
			return nil, nil
		}
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}
		switch write.Kind {
		case WriteKindPut:
			return txn.Reader.GetCF(engine_util.CfDefault, codec.EncodeKey(key, write.StartTS))
		case WriteKindDelete:
			return nil, nil
		case WriteKindLock | WriteKindRollback:
		}
	}

	// Iterated to the end of the DB
	return nil, nil
}

func (txn *MvccTxn) GetWrite(key []byte, ts uint64) (*Write, error) {
	// Since commit is not implemented, there are no writes in the DB.
	return nil, nil
}

// GetLock returns a lock if key is currently locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	bytes, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, nil
	}

	lock, err := ParseLock(bytes)
	if err != nil {
		return nil, err
	}

	return lock, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   codec.EncodeKey(key, *txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}
