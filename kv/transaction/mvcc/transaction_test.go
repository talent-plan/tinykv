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

func testTxn(startTs uint64) MvccTxn {
	mem := storage.NewMemStorage()
	reader, _ := mem.Reader(&kvrpcpb.Context{})
	return NewTxn(reader, startTs)
}

func assertPutInTxn(t *testing.T, txn *MvccTxn, key []byte, value []byte, cf string) {
	writes := txn.Writes()
	assert.Equal(t, 1, len(writes))
	assert.Equal(t, storage.ModifyTypePut, writes[0].Type)
	expected := storage.Put{Cf: cf, Key: key, Value: value}
	assert.Equal(t, expected, writes[0].Data.(storage.Put))
}

func TestPutLock4A(t *testing.T) {
	txn := testTxn(42)
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
	txn := testTxn(42)
	write := Write{
		StartTS: 100,
		Kind:    WriteKindDelete,
	}

	txn.PutWrite([]byte{1}, 42, &write)
	assertPutInTxn(t, &txn, EncodeKey([]byte{1}, 42), write.ToBytes(), engine_util.CfWrite)
}

func TestPutValue4A(t *testing.T) {
	txn := testTxn(42)
	value := []byte{1, 1, 2, 3, 5, 8, 13}

	txn.PutValue([]byte{1}, value)
	assertPutInTxn(t, &txn, EncodeKey([]byte{1}, 42), value, engine_util.CfDefault)
}
