package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/stretchr/testify/assert"
)

func TestEncodeKey(t *testing.T) {
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 247, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{}, 0))
	assert.Equal(t, []byte{42, 0, 0, 0, 0, 0, 0, 0, 248, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{42}, 0))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 0, 250, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{42, 0, 5}, 0))
	assert.Equal(t, []byte{42, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 39, 154, 52, 120, 65, 255}, EncodeKey([]byte{42}, ^uint64(43543258743295)))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 5, 226, 221, 76}, EncodeKey([]byte{42, 0, 5}, ^uint64(98753868)))

	// Test that encoded keys are in descending order.
	assert.True(t, bytes.Compare(EncodeKey([]byte{42}, 238), EncodeKey([]byte{200}, 0)) < 0)
	assert.True(t, bytes.Compare(EncodeKey([]byte{42}, 238), EncodeKey([]byte{42, 0}, 0)) < 0)
}

func TestDecodeKey(t *testing.T) {
	assert.Equal(t, []byte{}, DecodeUserKey(EncodeKey([]byte{}, 0)))
	assert.Equal(t, []byte{42}, DecodeUserKey(EncodeKey([]byte{42}, 0)))
	assert.Equal(t, []byte{42, 0, 5}, DecodeUserKey(EncodeKey([]byte{42, 0, 5}, 0)))
	assert.Equal(t, []byte{42}, DecodeUserKey(EncodeKey([]byte{42}, 2342342355436234)))
	assert.Equal(t, []byte{42, 0, 5}, DecodeUserKey(EncodeKey([]byte{42, 0, 5}, 234234)))
}

func testTxn(startTs uint64, f func(m *storage.MemStorage)) MvccTxn {
	mem := storage.NewMemStorage()
	if f != nil {
		f(mem)
	}
	reader, _ := mem.Reader(&kvrpcpb.Context{})
	return NewTxn(reader, startTs)
}

func assertPutInTxn(t *testing.T, txn *MvccTxn, key []byte, value []byte, cf string) {
	writes := txn.Writes()
	assert.Equal(t, 1, len(writes))
	expected := storage.Put{Cf: cf, Key: key, Value: value}
	put, ok := writes[0].Data.(storage.Put)
	assert.True(t, ok)
	assert.Equal(t, expected, put)
}

func assertDeleteInTxn(t *testing.T, txn *MvccTxn, key []byte, cf string) {
	writes := txn.Writes()
	assert.Equal(t, 1, len(writes))
	expected := storage.Delete{Cf: cf, Key: key}
	del, ok := writes[0].Data.(storage.Delete)
	assert.True(t, ok)
	assert.Equal(t, expected, del)
}

func TestPutLock4A(t *testing.T) {
	txn := testTxn(42, nil)
	lock := Lock{
		Primary: []byte{16},
		Ts:      100,
		Ttl:     100000,
		Kind:    WriteKindRollback,
	}

	txn.PutLock([]byte{1}, &lock)
	assertPutInTxn(t, &txn, []byte{1}, lock.ToBytes(), engine_util.CfLock)
}

func TestPutWrite4A(t *testing.T) {
	txn := testTxn(0, nil)
	write := Write{
		StartTS: 100,
		Kind:    WriteKindDelete,
	}

	txn.PutWrite([]byte{16, 240}, 0, &write)
	assertPutInTxn(t, &txn, EncodeKey([]byte{16, 240}, 0), write.ToBytes(), engine_util.CfWrite)
}

func TestPutValue4A(t *testing.T) {
	txn := testTxn(453325345, nil)
	value := []byte{1, 1, 2, 3, 5, 8, 13}

	txn.PutValue([]byte{32}, value)
	assertPutInTxn(t, &txn, EncodeKey([]byte{32}, 453325345), value, engine_util.CfDefault)
}

func TestGetLock4A(t *testing.T) {
	lock := Lock{
		Primary: []byte{16},
		Ts:      100,
		Ttl:     100000,
		Kind:    WriteKindRollback,
	}
	txn := testTxn(42, func(m *storage.MemStorage) {
		m.Set(engine_util.CfLock, []byte{1}, lock.ToBytes())
	})

	gotLock, err := txn.GetLock([]byte{1})
	assert.Nil(t, err)
	assert.Equal(t, lock, *gotLock)
}

func TestDeleteLock4A(t *testing.T) {
	txn := testTxn(42, nil)
	txn.DeleteLock([]byte{1})
	assertDeleteInTxn(t, &txn, []byte{1}, engine_util.CfLock)
}

func TestDeleteValue4A(t *testing.T) {
	txn := testTxn(63454245, nil)
	txn.DeleteValue([]byte{17, 255, 0})
	assertDeleteInTxn(t, &txn, EncodeKey([]byte{17, 255, 0}, 63454245), engine_util.CfDefault)
}

func singleEntry(m *storage.MemStorage) {
	m.Set(engine_util.CfDefault, EncodeKey([]byte{16, 240}, 40), []byte{1, 2, 3})
	write := Write{
		StartTS: 40,
		Kind:    WriteKindPut,
	}
	m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 42), write.ToBytes())
}

func TestGetValueSimple4A(t *testing.T) {
	txn := testTxn(43, singleEntry)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 2, 3}, value)
}

func TestGetValueMissing4A(t *testing.T) {
	txn := testTxn(43, singleEntry)

	value, err := txn.GetValue([]byte{16, 241})
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), value)
}

func TestGetValueTooEarly4A(t *testing.T) {
	txn := testTxn(41, singleEntry)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), value)
}

func twoEntries(m *storage.MemStorage) {
	m.Set(engine_util.CfDefault, EncodeKey([]byte{16, 240}, 40), []byte{1, 2, 3})
	write1 := Write{
		StartTS: 40,
		Kind:    WriteKindPut,
	}
	m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 42), write1.ToBytes())

	m.Set(engine_util.CfDefault, EncodeKey([]byte{16, 240}, 50), []byte{255, 0, 255})
	write2 := Write{
		StartTS: 50,
		Kind:    WriteKindPut,
	}
	m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 52), write2.ToBytes())
}

func TestGetValueOverwritten4A(t *testing.T) {
	txn := testTxn(52, twoEntries)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte{255, 0, 255}, value)
}

func TestGetValueNotOverwritten4A(t *testing.T) {
	txn := testTxn(50, twoEntries)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 2, 3}, value)
}

func deleted(m *storage.MemStorage) {
	m.Set(engine_util.CfDefault, EncodeKey([]byte{16, 240}, 40), []byte{1, 2, 3})
	write1 := Write{
		StartTS: 40,
		Kind:    WriteKindPut,
	}
	m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 42), write1.ToBytes())

	write2 := Write{
		StartTS: 50,
		Kind:    WriteKindDelete,
	}
	m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 52), write2.ToBytes())
}

func TestGetValueDeleted4A(t *testing.T) {
	txn := testTxn(500, deleted)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), value)
}

func TestGetValueNotDeleted4A(t *testing.T) {
	txn := testTxn(45, deleted)

	value, err := txn.GetValue([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 2, 3}, value)
}

func TestCurrentWrite4A(t *testing.T) {
	txn := testTxn(50, twoEntries)

	write, ts, err := txn.CurrentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 50,
		Kind:    WriteKindPut,
	}, *write)
	assert.Equal(t, uint64(52), ts)

	txn.StartTS = 40
	write, ts, err = txn.CurrentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 40,
		Kind:    WriteKindPut,
	}, *write)
	assert.Equal(t, uint64(42), ts)

	txn.StartTS = 41
	write, ts, err = txn.CurrentWrite([]byte{16, 240})
	assert.Nil(t, err)
	var noWrite *Write
	assert.Equal(t, noWrite, write)
	assert.Equal(t, uint64(0), ts)
}

func TestMostRecentWrite4A(t *testing.T) {
	// Empty DB.
	txn := testTxn(50, nil)
	write, ts, err := txn.MostRecentWrite([]byte{16, 240})
	assert.Nil(t, write)
	assert.Equal(t, uint64(0), ts)
	assert.Nil(t, err)

	// Simple case - key exists.
	txn = testTxn(50, twoEntries)
	write, ts, err = txn.MostRecentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 50,
		Kind:    WriteKindPut,
	}, *write)
	assert.Equal(t, uint64(52), ts)
	// No entry for other keys.
	write, ts, err = txn.MostRecentWrite([]byte{16})
	assert.Nil(t, write)
	assert.Equal(t, uint64(0), ts)
	assert.Nil(t, err)

	// Deleted key.
	txn = testTxn(50, deleted)
	write, ts, err = txn.MostRecentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 50,
		Kind:    WriteKindDelete,
	}, *write)
	assert.Equal(t, uint64(52), ts)

	// Result does not depend on txn ts.
	txn = testTxn(5000, twoEntries)
	write, ts, err = txn.MostRecentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 50,
		Kind:    WriteKindPut,
	}, *write)
	assert.Equal(t, uint64(52), ts)

	// Result does not depend on txn ts.
	txn = testTxn(1, twoEntries)
	write, ts, err = txn.MostRecentWrite([]byte{16, 240})
	assert.Nil(t, err)
	assert.Equal(t, Write{
		StartTS: 50,
		Kind:    WriteKindPut,
	}, *write)
	assert.Equal(t, uint64(52), ts)
}
