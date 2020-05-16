package standalone_storage

import (
	"fmt"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/stretchr/testify/require"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func TestNewStandAloneStorage(t *testing.T) {
	// type assert
	var _ storage.Storage = new(StandAloneStorage)
	conf := config.NewDefaultConfig()
	standalone := NewStandAloneStorage(conf)
	require.NotNil(t, standalone)

	reader, err := standalone.Reader(&kvrpcpb.Context{})
	require.NoError(t, err)
	require.NotNil(t, reader)

	err = standalone.badgerDB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("answer"), []byte("42"))
		return err
	})
	require.NoError(t, err)

	var item *badger.Item
	err = standalone.badgerDB.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte("answer"))
		fmt.Println(item.Key())
		fmt.Println(item.String())
		return err
	})
	require.Equal(t, item.Key(), []byte("answer"))
	v, err := item.Value()
	require.NoError(t, err)
	require.Equal(t, v, []byte("42"))
}

func TestReader(t *testing.T) {
	conf := config.NewDefaultConfig()
	standalone := NewStandAloneStorage(conf)

	key := createIndexKey(engine_util.CfDefault, "test")
	err := standalone.badgerDB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte("42"))
		return err
	})
	require.NoError(t, err)

	// set second value
	key = createIndexKey(engine_util.CfDefault, "test2")
	err = standalone.badgerDB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte("42"))
		return err
	})
	require.NoError(t, err)

	reader, err := standalone.Reader(&kvrpcpb.Context{})
	require.NoError(t, err)
	v, err := reader.GetCF(engine_util.CfDefault, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, v, []byte("42"))

	iterator := reader.IterCF(engine_util.CfDefault)
	require.NotNil(t, iterator)
	require.NotNil(t, iterator.Item())
	t.Log(string(iterator.Item().Key()))

	iterator.Next()
	// t.Log(string(iterator.Item().Key()))
	t.Log(string(iterator.Item().Key()))
	//iterator.Next()
	//t.Log(string(iterator.Item().Key()))
}
