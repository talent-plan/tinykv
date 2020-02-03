package kvstore

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
)

// Txn represents an mvcc transaction (see tikv/storage/doc.go for a definition). It permits reading from a snapshot
// and stores writes in a buffer for atomic writing.
type Txn struct {
	// TODO: is reader a snapshot or not?
	Reader  dbreader.DBReader
	Writes  []inner_server.Modify
	StartTS *uint64
}

func NewTxn(reader dbreader.DBReader) Txn {
	return Txn{
		reader,
		nil,
		nil,
	}
}

// SeekWrite returns the Write from the DB and the write's commit timestamp, or an error.
func (txn *Txn) SeekWrite(key []byte) (*Write, uint64, error) {
	return nil, 0, nil
}

// FindWrittenValue searches for a put write at key and with a timestamp at ts or later, if it finds one, it uses the
// write's timestamp to look up the value.
func (txn *Txn) FindWrittenValue(key []byte, ts uint64) ([]byte, error) {
	iter := txn.Reader.IterCF(inner_server.CfWrite)
	bts := [8]byte{}
	binary.BigEndian.PutUint64(bts[:], ts)
	for iter.Seek(AppendTS(key, ts)); iter.Valid(); iter.Next() {
		item := iter.Item()
		// If the user key part of the combined key has changed, then we've got to the next key without finding a put write.
		if bytes.Compare(ExtractUserKey(item.Key()), key) != 0 {
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
			return txn.Reader.GetCF(inner_server.CfDefault, AppendTS(key, write.StartTS))
		case WriteKindDelete:
			return nil, nil
		}
		// WriteKindLock or WriteKindRollback: continue
	}

	// Iterated to the end of the DB
	return nil, nil
}

func (txn *Txn) GetWrite(key []byte, ts uint64) (*Write, error) {
	// Since commit is not implemented, there are no writes in the DB.
	return nil, nil
}

// GetLock returns a lock if key is currently locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *Txn) GetLock(key []byte) (*Lock, error) {
	bytes, err := txn.Reader.GetCF(inner_server.CfLock, key)
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
func (txn *Txn) PutValue(key []byte, value []byte) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   AppendTS(key, *txn.StartTS),
			Value: value,
			Cf:    inner_server.CfDefault,
		},
	})
}

// PutLock adds a key/lock to this transaction.
func (txn *Txn) PutLock(key []byte, lock *Lock) {
	txn.Writes = append(txn.Writes, inner_server.Modify{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    inner_server.CfLock,
		},
	})
}

// AppendTS appends a timestamp to a key. Keys used by the transactional API are stored with their timestamps. I.e.,
// for every key, we store it's value at every point of time in the "default" CF.
func AppendTS(key []byte, ts uint64) []byte {
	newKey := append(key, 0, 0, 0, 0, 0, 0, 0, 0)
	// Note we invert the timestamp so that when sorted, they are in descending order.
	binary.BigEndian.PutUint64(newKey[len(key):], ^ts)
	return newKey
}

// ExtractUserKey takes a key + timestamp and returns the key part.
func ExtractUserKey(key []byte) []byte {
	keyLen := len(key) - 8
	return key[:keyLen]
}
