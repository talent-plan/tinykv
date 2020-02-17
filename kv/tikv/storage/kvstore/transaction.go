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

// SeekWrite finds the write with the given key and the most recent timestamp before or equal to ts. If ts is TsMax, then
// it will find the most recent write for key. It returns a Write from the DB and that write's commit timestamp, or an error.
// Postcondition: the returned ts is <= the ts arg.
func (txn *MvccTxn) SeekWrite(key []byte, ts uint64) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, ts))
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()
	commitTs := decodeTimestamp(item.Key())
	if bytes.Compare(decodeUserKey(item.Key()), key) != 0 {
		return nil, 0, nil
	}
	value, err := item.Value()
	if err != nil {
		return nil, 0, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return nil, 0, err
	}

	return write, commitTs, nil
}

// FindWrite searches for a write at exactly startTs.
func (txn *MvccTxn) FindWrite(key []byte, startTs uint64) (*Write, uint64, error) {
	seekTs := TsMax
	for {
		write, commitTs, err := txn.SeekWrite(key, seekTs)
		if err != nil {
			return nil, 0, err
		}
		if write == nil {
			return nil, 0, nil
		}
		if write.StartTS == startTs {
			return write, commitTs, nil
		}
		if commitTs <= startTs {
			return nil, 0, nil
		}
		seekTs = commitTs - 1
	}
}

// FindWrittenValue searches for a put write at key and with a timestamp at ts or later, if it finds one, it uses the
// write's timestamp to look up the value.
func (txn *MvccTxn) FindWrittenValue(key []byte, ts uint64) ([]byte, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	bts := [8]byte{}
	binary.BigEndian.PutUint64(bts[:], ts)
	for iter.Seek(EncodeKey(key, ts)); iter.Valid(); iter.Next() {
		item := iter.Item()
		// If the user key part of the combined key has changed, then we've got to the next key without finding a put write.
		if bytes.Compare(decodeUserKey(item.Key()), key) != 0 {
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
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		case WriteKindDelete:
			return nil, nil
		case WriteKindRollback:
		}
	}

	// Iterated to the end of the DB
	return nil, nil
}

// GetWrite gets the write at precisely the given key and ts, without searching.
func (txn *MvccTxn) GetWrite(key []byte, ts uint64) (*Write, error) {
	value, err := txn.Reader.GetCF(engine_util.CfWrite, EncodeKey(key, ts))
	if err != nil {
		return nil, err
	}
	return ParseWrite(value)
}

func (txn *MvccTxn) PutWrite(key []byte, write *Write, ts uint64) {
	encodedKey := EncodeKey(key, ts)
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   encodedKey,
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
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

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue gets the value at precisely the given key and ts, without searching.
func (txn *MvccTxn) GetValue(key []byte, ts uint64) ([]byte, error) {
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, ts))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   EncodeKey(key, *txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: EncodeKey(key, *txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	})
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// decodeUserKey takes a key + timestamp and returns the key part.
func decodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}
