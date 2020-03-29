package engine_util

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/stretchr/testify/require"
)

func TestEngineUtil(t *testing.T) {
	dir, err := ioutil.TempDir("", "engine_util")
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)

	batch := new(WriteBatch)
	batch.SetCF(CfDefault, []byte("a"), []byte("a1"))
	batch.SetCF(CfDefault, []byte("b"), []byte("b1"))
	batch.SetCF(CfDefault, []byte("c"), []byte("c1"))
	batch.SetCF(CfDefault, []byte("d"), []byte("d1"))
	batch.SetCF(CfWrite, []byte("a"), []byte("a2"))
	batch.SetCF(CfWrite, []byte("b"), []byte("b2"))
	batch.SetCF(CfWrite, []byte("d"), []byte("d2"))
	batch.SetCF(CfLock, []byte("a"), []byte("a3"))
	batch.SetCF(CfLock, []byte("c"), []byte("c3"))
	batch.SetCF(CfDefault, []byte("e"), []byte("e1"))
	batch.DeleteCF(CfDefault, []byte("e"))
	err = batch.WriteToDB(db)
	require.Nil(t, err)

	_, err = GetCF(db, CfDefault, []byte("e"))
	require.Equal(t, err, badger.ErrKeyNotFound)

	err = PutCF(db, CfDefault, []byte("e"), []byte("e2"))
	require.Nil(t, err)
	val, _ := GetCF(db, CfDefault, []byte("e"))
	require.Equal(t, val, []byte("e2"))
	err = DeleteCF(db, CfDefault, []byte("e"))
	require.Nil(t, err)
	_, err = GetCF(db, CfDefault, []byte("e"))
	require.Equal(t, err, badger.ErrKeyNotFound)

	txn := db.NewTransaction(false)
	defer txn.Discard()
	defaultIter := NewCFIterator(CfDefault, txn)
	defaultIter.Seek([]byte("a"))
	item := defaultIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("a")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("a1")))
	defaultIter.Next()
	item = defaultIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("b")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("b1")))
	defaultIter.Next()
	item = defaultIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("c")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("c1")))
	defaultIter.Next()
	item = defaultIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("d")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("d1")))
	defaultIter.Next()
	require.False(t, defaultIter.Valid())
	defaultIter.Close()

	writeIter := NewCFIterator(CfWrite, txn)
	writeIter.Seek([]byte("b"))
	item = writeIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("b")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("b2")))
	writeIter.Next()
	item = writeIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("d")))
	val, _ = item.Value()
	require.True(t, bytes.Equal(val, []byte("d2")))
	writeIter.Next()
	require.False(t, writeIter.Valid())
	writeIter.Close()

	lockIter := NewCFIterator(CfLock, txn)
	lockIter.Seek([]byte("d"))
	require.False(t, lockIter.Valid())
	lockIter.Close()
}
