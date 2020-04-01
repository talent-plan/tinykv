package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxnStub struct{}

// Writes returns all changes added to this transaction.
func (txn *MvccTxnStub) Writes() []storage.Modify {
	return nil
}

// PutWrite records a write at key and ts.
func (txn *MvccTxnStub) PutWrite(key []byte, ts uint64, write *Write) {
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxnStub) GetLock(key []byte) (*Lock, error) {
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxnStub) PutLock(key []byte, lock *Lock) {
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxnStub) DeleteLock(key []byte) {
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxnStub) GetValue(key []byte) ([]byte, error) {
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxnStub) PutValue(key []byte, value []byte) {
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxnStub) DeleteValue(key []byte) {
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxnStub) CurrentWrite(key []byte) (*Write, uint64, error) {
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxnStub) MostRecentWrite(key []byte) (*Write, uint64, error) {
	return nil, 0, nil
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

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
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

// TODO delete all code below this comment and rename MvccTxnStub to MvccTxn.

// MvccTxn represents an mvcc transaction (see tikv/storage/doc.go for a definition). It permits reading from a snapshot
// and stores writes in a buffer for atomic writing.
type MvccTxn struct {
	RoTxn
	writes []storage.Modify
}

// A 'transaction' which will only read from the DB.
type RoTxn struct {
	Reader  storage.StorageReader
	StartTS uint64
}

func NewTxn(reader storage.StorageReader, startTs uint64) MvccTxn {
	return MvccTxn{
		RoTxn: RoTxn{Reader: reader, StartTS: startTs},
	}
}

func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *RoTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	return txn.mostRecentWriteBefore(key, TsMax)
}

// mostRecentWriteBefore finds the write with the given key and the most recent commit timestamp before or equal to ts.
// It returns a Write from the DB and that write's commit timestamp, or an error.
// Postcondition: the returned ts is <= the ts arg.
func (txn *RoTxn) mostRecentWriteBefore(key []byte, ts uint64) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, ts))
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()
	commitTs := decodeTimestamp(item.Key())
	if bytes.Compare(DecodeUserKey(item.Key()), key) != 0 {
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

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *RoTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	seekTs := TsMax
	for {
		write, commitTs, err := txn.mostRecentWriteBefore(key, seekTs)
		if err != nil {
			return nil, 0, err
		}
		if write == nil {
			return nil, 0, nil
		}
		if write.StartTS == txn.StartTS {
			return write, commitTs, nil
		}
		if commitTs <= txn.StartTS {
			return nil, 0, nil
		}
		seekTs = commitTs - 1
	}
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *RoTxn) GetValue(key []byte) ([]byte, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	bts := [8]byte{}
	ts := txn.StartTS
	binary.BigEndian.PutUint64(bts[:], ts)
	for iter.Seek(EncodeKey(key, ts)); iter.Valid(); iter.Next() {
		item := iter.Item()
		// If the user key part of the combined key has changed, then we've got to the next key without finding a put write.
		if bytes.Compare(DecodeUserKey(item.Key()), key) != 0 {
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

// PutWrite records write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	encodedKey := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   encodedKey,
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *RoTxn) GetLock(key []byte) (*Lock, error) {
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
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// getValue gets the value at precisely the given key and ts, without searching.
func (txn *RoTxn) getValue(key []byte, ts uint64) ([]byte, error) {
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, ts))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	})
}
