package engine_util

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/coocood/badger"
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
	batch.SetCF(CF_DEFAULT, []byte("a"), []byte("a1"))
	batch.SetCF(CF_DEFAULT, []byte("b"), []byte("b1"))
	batch.SetCF(CF_DEFAULT, []byte("c"), []byte("c1"))
	batch.SetCF(CF_DEFAULT, []byte("d"), []byte("d1"))
	batch.SetCF(CF_WRITE, []byte("a"), []byte("a2"))
	batch.SetCF(CF_WRITE, []byte("b"), []byte("b2"))
	batch.SetCF(CF_WRITE, []byte("d"), []byte("d2"))
	batch.SetCF(CF_LOCK, []byte("a"), []byte("a3"))
	batch.SetCF(CF_LOCK, []byte("c"), []byte("c3"))
	batch.Set([]byte("a"), []byte("a0"))
	batch.Delete([]byte("a"))
	batch.SetCF(CF_DEFAULT, []byte("e"), []byte("e1"))
	batch.DeleteCF(CF_DEFAULT, []byte("e"))
	err = batch.WriteToDB(db)
	require.Nil(t, err)

	_, err = GetCF(db, CF_DEFAULT, []byte("e"))
	require.Equal(t, err, badger.ErrKeyNotFound)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	defaultIter := NewCFIterator(CF_DEFAULT, txn)
	defaultIter.Seek([]byte("a"))
	item := defaultIter.Item()
	require.True(t, bytes.Equal(item.Key(), []byte("a")))
	val, _ := item.Value()
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

	writeIter := NewCFIterator(CF_WRITE, txn)
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

	lockIter := NewCFIterator(CF_LOCK, txn)
	lockIter.Seek([]byte("d"))
	require.False(t, lockIter.Valid())
	lockIter.Close()
}
