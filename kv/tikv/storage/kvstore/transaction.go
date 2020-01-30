package kvstore

import (
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
)

// Txn represents an mvcc transaction (see tikv/storage/doc.go for a definition). It permits reading from a snapshot
// and stores writes in a buffer for atomic writing.
type Txn struct {
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
func (txn *Txn) SeekWrite(key []byte, ts ...uint64) (*Write, uint64, error) {
	// Since commit is not implemented, there are no writes in the DB.
	return nil, 0, nil
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
			Key:   appendTS(key, *txn.StartTS),
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

// appendTS appends a timestamp to a key. Keys used by the transactional API are stored with their timestamps. I.e.,
// for every key, we store it's value at every point of time in the "default" CF.
func appendTS(key []byte, ts uint64) []byte {
	newKey := append(key, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(newKey[len(key):], ts)
	return newKey
}
